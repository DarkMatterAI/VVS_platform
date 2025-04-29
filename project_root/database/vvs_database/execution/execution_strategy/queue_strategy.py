from __future__ import annotations
import asyncio, time, random
from typing import Dict, List, Tuple

from vvs_database import logging
from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
from vvs_database.execution.execution_strategy.state_models import (
    QueueRequestState,
    StrategyResponse,
    QueueConst,
)
from vvs_database.execution.execution_strategy.utils import _chunk
from vvs_database.execution.connections import Connections
from vvs_database.schemas import (
    ExecuteParams,
    ExecuteRequestUnion,
    ExecuteResponseUnion,
    ExecutionSources,
    PluginInDB,
)


class QueueExecutionStrategy(ExecutionStrategy):
    """
    RabbitMQ publisher + Redis poller with optional semaphore throttling.
    """

    # ───────────────────────────────────────────────────────────────── #
    # life-cycle                                                      #
    # ───────────────────────────────────────────────────────────────── #
    def __init__(self, connections: Connections, params: ExecuteParams):
        super().__init__(connections, params)
        self.redis   = connections.redis_service
        self.rabbit  = connections.rabbitmq_service
        self.params  = params
        self.log_id  = "QueueExecute"

    # ───────────────────────────────────────────────────────────────── #
    # public API                                                      #
    # ───────────────────────────────────────────────────────────────── #
    async def execute(
        self,
        plugin:   PluginInDB,
        requests: Dict[str, ExecuteRequestUnion],
    ) -> Dict[str, ExecuteResponseUnion]:

        if not requests:
            return {}

        const    = QueueConst.from_plugin(plugin, self.params)
        tracker  = self._init_tracker(requests, const)

        while self._unfinished(tracker):
            pending_batches = self._waiting_batches(tracker, const.bs)

            tokens = await self._acquire_tokens(pending_batches, const)
            if await self._maybe_backoff(tokens, pending_batches, const, tracker):
                continue

            wave_batches, token_list = self._pick_wave(pending_batches, tokens, const)
            self._mark_processing(wave_batches, token_list, tracker)
            await asyncio.gather(
                *[self._publish_batch(b, tracker, plugin) for b in wave_batches]
            )
            await self._release_tokens(tokens, const)

            await self._poll_results(tracker, const)
            self._apply_timeouts(tracker, const)
            await asyncio.sleep(const.poll_interval)

        return {k: st.response.as_legacy_dict() for k, st in tracker.items()}

    # ───────────────────────────────────────────────────────────────── #
    # helper sections (typed)                                          #
    # ───────────────────────────────────────────────────────────────── #

    # ----- tracker construction ------------------------------------- #
    def _init_tracker(
        self,
        requests: Dict[str, ExecuteRequestUnion],
        const: QueueConst,
    ) -> Dict[str, QueueRequestState]:
        tracker: Dict[str, QueueRequestState] = {}
        for k, r in requests.items():
            rid  = r.request_data.request_id
            resp = rid.replace("request", "response").replace(".", ":")
            tracker[k] = QueueRequestState(
                key=k,
                request=r,
                req_id=rid,
                resp_id=resp,
                attempts_left=const.max_attempts,
            )
        return tracker

    # ----- status helpers ------------------------------------------- #
    @staticmethod
    def _unfinished(tracker: Dict[str, QueueRequestState]) -> bool:
        return any(st.status in ("waiting", "processing", "queued") for st in tracker.values())

    @staticmethod
    def _waiting_batches(tracker: Dict[str, QueueRequestState], bs: int) -> List[List[str]]:
        keys = [
            k for k, st in tracker.items()
            if st.status in ("waiting", "processing")
        ]
        return _chunk(keys, bs)

    # ----- semaphore helpers ---------------------------------------- #
    async def _acquire_tokens(
        self,
        pending_batches: List[List[str]],
        const: QueueConst,
    ) -> List[str]:
        if not const.use_sema or not pending_batches:
            return []
        need = min(len(pending_batches), const.max_conc)
        return await self.redis.acquire_semaphores_batch(
            const.sem_name, need, const.max_conc, lock_timeout=const.lock_t
        )

    async def _release_tokens(self, tokens: List[str], const: QueueConst) -> None:
        if tokens and const.use_sema:
            await self.redis.release_semaphore(const.sem_name, tokens)

    # ----- back-off check ------------------------------------------- #
    async def _maybe_backoff(
        self,
        tokens: List[str],
        pending_batches: List[List[str]],
        const: QueueConst,
        tracker: Dict[str, QueueRequestState],
    ) -> bool:
        """
        Returns True  if we hit a back-off condition and have *already* slept,
        False if normal processing should continue.
        """
        if const.use_sema and not tokens and pending_batches:
            for k in self._waiting_batches(tracker, const.bs)[0]:
                st = tracker[k]
                st.attempts_left -= 1
                if st.attempts_left <= 0:
                    st.status   = "error"
                    st.response = StrategyResponse.failure(
                        "Semaphore failure", "Exceeded max attempts"
                    )
            await asyncio.sleep(const.backoff_sleep())
            return True
        return False

    # ----- wave selection ------------------------------------------- #
    def _pick_wave(
        self,
        pending_batches: List[List[str]],
        tokens: List[str],
        const: QueueConst,
    ) -> Tuple[List[List[str]], List[str | None]]:
        if const.use_sema:
            n_wave     = len(tokens)
            token_list = tokens
        else:
            n_wave     = min(const.max_conc, len(pending_batches))
            token_list = [None] * n_wave
        wave_batches = [pending_batches.pop(0) for _ in range(n_wave)]
        return wave_batches, token_list

    def _mark_processing(
        self,
        wave_batches: List[List[str]],
        token_list: List[str | None],
        tracker: Dict[str, QueueRequestState],
    ) -> None:
        for batch_keys, tok in zip(wave_batches, token_list):
            for k in batch_keys:
                st = tracker[k]
                st.status     = "processing"
                st.identifier = tok

    # ----- rabbit/redis ops ----------------------------------------- #
    async def _publish_batch(
        self,
        keys: List[str],
        tracker: Dict[str, QueueRequestState],
        plugin: PluginInDB,
    ) -> None:
        msgs = [tracker[k].request for k in keys]
        try:
            sent = self.rabbit.publish_messages(msgs)
        except Exception as exc:
            logging.error("%s: RabbitMQ publish failed – %s", self.log_id, exc)
            sent = []

        now = time.time()
        sent_set = set(sent)
        for k in keys:
            st = tracker[k]
            if st.req_id in sent_set:
                st.status   = "queued"
                st.queued_at = now
            else:
                st.queue_errors += 1

    async def _poll_results(
        self,
        tracker: Dict[str, QueueRequestState],
        const: QueueConst,
    ) -> None:
        ids = [st.resp_id for st in tracker.values() if st.status == "queued"]
        if not ids:
            return
        hits = await self.redis.get_results(ids, delete=True)
        id_map = {st.resp_id: st for st in tracker.values()}
        for rid, payload in hits.items():
            st = id_map[rid]
            st.status   = "done"
            st.response = StrategyResponse(
                **payload,
                # valid=True,
                # response_data=payload,
                source=ExecutionSources.EXECUTION,
            )

    # ----- time-out handling ---------------------------------------- #
    def _apply_timeouts(
        self,
        tracker: Dict[str, QueueRequestState],
        const: QueueConst,
    ) -> None:
        now = time.time()
        for st in tracker.values():
            if st.status in ("done", "error"):
                continue
            if st.attempts_left <= 0:
                st.status   = "error"
                st.response = StrategyResponse.failure(
                    "Semaphore failure", "Exceeded max attempts"
                )
            elif st.queue_errors > 3:
                st.status   = "error"
                st.response = StrategyResponse.failure(
                    "Queue error", "Exceeded publish retries"
                )
            elif st.status == "queued" and st.queued_at and (now - st.queued_at) > const.timeout:
                st.status   = "error"
                st.response = StrategyResponse.failure(
                    "Queue timeout", "No response within timeout"
                )



# from __future__ import annotations
# import asyncio, time, random
# from typing import Dict, List

# from vvs_database import logging
# from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
# from vvs_database.execution.execution_strategy.state_models import (
#     QueueRequestState,
#     StrategyResponse,
# )
# from vvs_database.execution.execution_strategy.utils import _chunk
# from vvs_database.execution.connections import Connections
# from vvs_database.schemas import (
#     ExecuteParams,
#     ExecuteRequestUnion,
#     ExecuteResponseUnion,
#     ExecutionSources,
#     PluginInDB,
# )


# class QueueExecutionStrategy(ExecutionStrategy):
#     """
#     RabbitMQ + Redis polling implementation with Redis semaphore
#     throttling.
#     """

#     # ------------- life-cycle ------------------------------------------- #
#     def __init__(self, connections: Connections, params: ExecuteParams):
#         super().__init__(connections, params)
#         self.redis   = connections.redis_service
#         self.rabbit  = connections.rabbitmq_service
#         self.params  = params
#         self.log_id  = "QueueExecute"

#     # ------------- public API ------------------------------------------- #
#     async def execute(
#         self,
#         plugin: PluginInDB,
#         requests: Dict[str, ExecuteRequestUnion],
#     ) -> Dict[str, ExecuteResponseUnion]:

#         if not requests:
#             return {}

#         timeout       = plugin.timeout
#         lock_t        = int(timeout * 1.1)
#         max_conc      = plugin.max_concurrency
#         bs            = plugin.batch_size
#         sem_name      = f"plugin:{plugin.id}"
#         poll_interval = self.params.queue_polling_interval
#         backoff_base  = max(0.5, min(2.0, timeout / max_conc))
#         backoff_cur   = backoff_base

#         # ----- build tracker --------------------------------------------
#         tracker: Dict[str, QueueRequestState] = {}
#         for k, r in requests.items():
#             rid = r.request_data.request_id
#             tracker[k] = QueueRequestState(
#                 key=k,
#                 request=r,
#                 req_id=rid,
#                 resp_id=rid.replace("request", "response").replace(".", ":"),
#                 attempts_left=self.params.max_semaphore_attempts,
#             )

#         def _waiting_keys() -> List[str]:
#             return [
#                 k for k, st in tracker.items()
#                 if st.status in ("waiting", "processing")
#             ]

#         pending_batches = _chunk(list(tracker), bs)

#         # ----- main loop -------------------------------------------------
#         while any(st.status in ("waiting", "processing", "queued") for st in tracker.values()):

#             pending_batches = _chunk(_waiting_keys(), bs)

#             # ------------------------------------------------------------------
#             # acquire tokens (may be empty list when semaphore disabled)
#             tokens: List[str] = []
#             if self.params.use_semaphore and pending_batches:
#                 need = min(len(pending_batches), max_conc)
#                 tokens = await self.redis.acquire_semaphores_batch(
#                     sem_name, need, max_conc, lock_timeout=lock_t
#                 )

#             if self.params.use_semaphore and not tokens and pending_batches:
#                 # no tokens → back-off
#                 for k in _waiting_keys():
#                     st = tracker[k]
#                     st.attempts_left -= 1
#                     if st.attempts_left <= 0:
#                         st.status   = "error"
#                         st.response = StrategyResponse.failure(
#                             "Semaphore failure", "Exceeded max attempts"
#                         )
#                 await asyncio.sleep(backoff_cur * (0.8 + random.random() * 0.4))
#                 backoff_cur = min(backoff_cur * self.params.backoff_factor, timeout)
#                 continue

#             backoff_cur = backoff_base  # progress reset

#             # ──────────────────────────────────────────────────────────────
#             # Decide how many batches we can publish this wave and pick them
#             # ──────────────────────────────────────────────────────────────
#             if not pending_batches:
#                 # nothing to publish – just poll / timeout
#                 await self._poll_results(tracker)
#                 self._apply_timeouts(tracker, timeout)
#                 await asyncio.sleep(poll_interval)
#                 continue

#             if self.params.use_semaphore:
#                 n_wave  = len(tokens)                       # exactly 1 batch per real token
#                 token_list = tokens                         # real identifiers
#             else:
#                 n_wave  = min(max_conc, len(pending_batches))
#                 token_list = [None] * n_wave                # dummy tokens → no identifier

#             wave_keys = [pending_batches.pop(0) for _ in range(n_wave)]

#             # mark state and remember identifier
#             for batch_keys, tok in zip(wave_keys, token_list):
#                 for k in batch_keys:
#                     st = tracker[k]
#                     st.status     = "processing"
#                     st.identifier = tok

#             await asyncio.gather(
#                 *[self._publish_batch(batch_keys, tracker, plugin) for batch_keys in wave_keys]
#             )

#             if tokens and self.params.use_semaphore:
#                 await self.redis.release_semaphore(sem_name, tokens)

#             # ------------------------------------------------------------------
#             # poll & timeout handling
#             await self._poll_results(tracker)
#             self._apply_timeouts(tracker, timeout)

#             await asyncio.sleep(poll_interval)

#         # ----- aggregate --------------------------------------------------
#         return {
#             k: st.response.as_legacy_dict() if st.response else StrategyResponse.failure(
#                 "Unknown", "Internal error"
#             ).as_legacy_dict()
#             for k, st in tracker.items()
#         }

#     # ------------- helpers --------------------------------------------- #
#     async def _publish_batch(
#         self,
#         keys: List[str],
#         tracker: Dict[str, QueueRequestState],
#         plugin: PluginInDB,
#     ):
#         msgs = [tracker[k].request for k in keys]
#         try:
#             sent = self.rabbit.publish_messages(msgs)
#         except Exception as exc:
#             logging.error("%s: RabbitMQ publish failed - %s", self.log_id, exc)
#             sent = []

#         now = time.time()
#         sent_set = set(sent)
#         for k in keys:
#             st = tracker[k]
#             if st.req_id in sent_set:
#                 st.status   = "queued"
#                 st.queued_at = now
#             else:
#                 st.queue_errors += 1

#     async def _poll_results(self, tracker: Dict[str, QueueRequestState]):
#         ids = [st.resp_id for st in tracker.values() if st.status == "queued"]
#         if not ids:
#             return
#         hits = await self.redis.get_results(ids, delete=True)
#         for rid, payload in hits.items():
#             for st in tracker.values():
#                 if st.resp_id == rid:
#                     st.status   = "done"
#                     st.response = StrategyResponse(
#                         **payload,
#                         source=ExecutionSources.EXECUTION,
#                     )
#                     break

#     def _apply_timeouts(self, tracker: Dict[str, QueueRequestState], timeout_s: int):
#         now = time.time()
#         for st in tracker.values():
#             if st.status in ("done", "error"):
#                 continue
#             if st.attempts_left <= 0:
#                 st.status   = "error"
#                 st.response = StrategyResponse.failure(
#                     "Semaphore failure", "Exceeded max attempts"
#                 )
#             elif st.queue_errors > 3:
#                 st.status   = "error"
#                 st.response = StrategyResponse.failure(
#                     "Queue error", "Exceeded publish retries"
#                 )
#             elif st.status == "queued" and st.queued_at and (now - st.queued_at) > timeout_s:
#                 st.status   = "error"
#                 st.response = StrategyResponse.failure(
#                     "Queue timeout", "No response within timeout"
#                 )





# # import asyncio
# # import time
# # import random
# # from typing import Dict, List, Tuple

# # from vvs_database.schemas import (
# #     ExecuteRequestUnion,
# #     ExecuteResponseUnion,
# #     ExecuteParams,
# #     PluginInDB,
# #     ExecutionSources
# # )
# # from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
# # from vvs_database.execution.connections import Connections
# # from vvs_database import logging


# # class QueueExecutionStrategy(ExecutionStrategy):
# #     """
# #     Execute requests via RabbitMQ and poll Redis for responses.
# #     Each *batch* of messages is published once a semaphore token has been
# #     obtained; tokens are acquired in bulk to minimise Redis chatter.
# #     """

# #     # --------------------------- life-cycle ---------------------------------

# #     def __init__(self, connections: Connections, execute_params: ExecuteParams):
# #         self.redis_service = connections.redis_service
# #         self.rabbitmq_service = connections.rabbitmq_service
# #         self.execute_params = execute_params
# #         self.log_id = "QueueExecute"

# #     # --------------------------- public API ---------------------------------

# #     async def execute(
# #         self,
# #         plugin: PluginInDB,
# #         requests: Dict[str, ExecuteRequestUnion],
# #     ) -> Dict[str, ExecuteResponseUnion]:
# #         logging.info(f"{self.log_id}: Queueing {len(requests.keys())} requests via RabbitMQ")
# #         if not requests:
# #             return {}

# #         # ── plugin-level knobs ─────────────────────────────────────────────
# #         timeout          = plugin.timeout                     # sec
# #         lock_timeout     = int(timeout * 1.1)
# #         max_concurrency  = plugin.max_concurrency
# #         batch_size       = plugin.batch_size
# #         semaphore_name   = f"plugin:{plugin.id}"

# #         # ── build RequestTracker dict ─────────────────────────────────────
# #         tracker: Dict[str, dict] = {}
# #         for k, req in requests.items():
# #             rid = req.request_data.request_id
# #             tracker[k] = {
# #                 "request":        req,
# #                 "req_id":         rid,
# #                 "resp_id":        rid.replace("request", "response").replace(".", ":"),
# #                 "status":         "waiting",     # waiting | queued | done | error
# #                 "queued_at":      None,
# #                 "identifier":     None,          # semaphore token
# #                 "attempts_left":  self.execute_params.max_semaphore_attempts,
# #                 "queue_errors":   0,
# #                 "result":         None,
# #             }

# #         # split requests into *batches* that will share one token
# #         pending_batches: List[List[str]] = []   # list[ list[key] ]
# #         objs = list(tracker.keys())
# #         for i in range(0, len(objs), batch_size):
# #             pending_batches.append(objs[i : i + batch_size])

# #         # ── constants for back-off & timeout checks ───────────────────────
# #         poll_interval   = self.execute_params.queue_polling_interval
# #         base_backoff    = max(0.5, min(2.0, timeout / max_concurrency))
# #         backoff_factor  = self.execute_params.backoff_factor
# #         max_queue_err   = 3

# #         # ── main loop: continue while ANY request unfinished ──────────────
# #         backoff_current = base_backoff

# #         while True:
# #             unfinished = [
# #                 k for k, v in tracker.items()
# #                 if v["status"] in ("waiting", "processing", "queued")
# #             ]
# #             if not unfinished:
# #                 break   # all done / error ➜ exit

# #             # ----------------------------------------------------------
# #             # Build (or rebuild) batches that still need publishing
# #             # ----------------------------------------------------------
# #             pending_batches = self._rebuild_waiting_batches(tracker, batch_size)
# #             poll_count = len(unfinished) - len(pending_batches)
# #             logging.info(f"{self.log_id}: Queue loop - {len(pending_batches)} unpublished, {poll_count} outstanding")

# #             # ───── No more to publish → just poll/timeout & sleep ─────
# #             if not pending_batches:
# #                 await self._poll_results(plugin, tracker)
# #                 self._apply_timeouts_and_errors(tracker, timeout, max_queue_err)
# #                 await asyncio.sleep(poll_interval)
# #                 continue

# #             # ----------------------------------------------------------
# #             # 1) Acquire semaphore tokens (or dummy tokens)
# #             # ----------------------------------------------------------
# #             n_need  = min(len(pending_batches), max_concurrency)
# #             if self.execute_params.use_semaphore:
# #                 tokens = await self.redis_service.acquire_semaphores_batch(
# #                     name=semaphore_name,
# #                     n=n_need,
# #                     max_locks=max_concurrency,
# #                     lock_timeout=lock_timeout,
# #                 )
# #             else:
# #                 tokens = [None] * n_need        # semaphore disabled

# #             if not tokens:                      # no tokens this round
# #                 await self._handle_no_tokens(pending_batches, tracker)
# #                 jitter = 0.8 + random.random() * 0.4
# #                 await asyncio.sleep(backoff_current * jitter)
# #                 backoff_current = min(backoff_current * backoff_factor, timeout)
# #                 self._apply_timeouts_and_errors(tracker, timeout, max_queue_err)
# #                 continue

# #             backoff_current = base_backoff      # progress → reset back‑off

# #             # ----------------------------------------------------------
# #             # 2) Launch wave for len(tokens) batches
# #             # ----------------------------------------------------------
# #             wave_batches = [pending_batches.pop(0) for _ in range(len(tokens))]

# #             # mark “processing” + remember token
# #             for batch_keys, tok in zip(wave_batches, tokens):
# #                 for k in batch_keys:
# #                     tracker[k]["status"] = "processing"
# #                     tracker[k]["identifier"] = tok

# #             await asyncio.gather(
# #                 *[self._publish_batch(plugin, b, tracker) for b in wave_batches]
# #             )

# #             # release real tokens
# #             real_tokens = [t for t in tokens if t]
# #             if self.execute_params.use_semaphore and real_tokens:
# #                 await self.redis_service.release_semaphore(semaphore_name, real_tokens)

# #             # ----------------------------------------------------------
# #             # 3) After publishing, poll / handle errors immediately
# #             # ----------------------------------------------------------
# #             await self._poll_results(plugin, tracker)
# #             self._apply_timeouts_and_errors(tracker, timeout, max_queue_err)

# #         # ── compile final dict[orig_key] → ExecuteResponseUnion ────────────
# #         return {k: v["result"] for k, v in tracker.items()}

# #     # --------------------------- helpers -----------------------------------

# #     async def _publish_batch(self, plugin, batch_keys: List[str], tracker: Dict[str, dict]):
# #         """Try to publish a batch; on failure increment queue_errors per key."""
# #         messages = [tracker[k]["request"] for k in batch_keys]
# #         try:
# #             sent_ids = self.rabbitmq_service.publish_messages(messages)
# #         except Exception as exc:  # network / channel failure
# #             logging.error("%s: RabbitMQ publish failed - %s", self.log_id, exc)
# #             sent_ids = []

# #         now = time.time()
# #         sent_set = set(sent_ids)
# #         for k in batch_keys:
# #             if tracker[k]["req_id"] in sent_set:
# #                 tracker[k]["status"] = "queued"
# #                 tracker[k]["queued_at"] = now
# #             else:  # failed publish
# #                 tracker[k]["queue_errors"] += 1

# #     async def _poll_results(self, plugin, tracker: Dict[str, dict]):
# #         """Fetch any available responses from Redis in one MGET."""
# #         polling_ids = [
# #             v["resp_id"] for v in tracker.values() if v["status"] == "queued"
# #         ]
# #         if not polling_ids:
# #             return 
# #         res = await self.redis_service.get_results(polling_ids, delete=True)
# #         for resp_id, payload in res.items():
# #             for k, meta in tracker.items():
# #                 if meta["resp_id"] == resp_id:
# #                     meta["status"] = "done"
# #                     meta["result"] = payload
# #                     meta["result"]["source"] = ExecutionSources.EXECUTION
# #                     # 1/0
# #                     break

# #     def _apply_timeouts_and_errors(self, tracker, timeout, max_queue_err):
# #         """Mark requests error if queue timeout or too many publish failures."""
# #         now = time.time()
# #         for meta in tracker.values():
# #             if meta["status"] == "done" or meta["status"] == "error":
# #                 continue

# #             # semaphore exhaustion
# #             if meta["attempts_left"] <= 0:
# #                 logging.error(f"{self.log_id}: Message failed to acquire semaphore")
# #                 self._fail(meta, "Semaphore failure",
# #                            f"Exceeded max attempts ({self.execute_params.max_semaphore_attempts})")
# #                 continue

# #             # publish failures
# #             if meta["queue_errors"] > max_queue_err:
# #                 logging.error(f"{self.log_id}: Message failed to post to queue")
# #                 self._fail(meta, "Queue error", "Exceeded publish retries")
# #                 continue

# #             # queue timeout
# #             if meta["status"] == "queued" and meta["queued_at"] is not None:
# #                 if now - meta["queued_at"] > timeout:
# #                     logging.error(f"{self.log_id}: Message failed timeout")
# #                     self._fail(meta, "Queue timeout", "No response within timeout")

# #     def _fail(self, meta, reason, detail):
# #         meta["status"] = "error"
# #         meta["result"] = {
# #             "valid": False,
# #             "response_data": None,
# #             "failure_reason": reason,
# #             "failure_detail": detail,
# #             "source": ExecutionSources.FAILURE
# #         }

# #     async def _handle_no_tokens(self, pending_batches, tracker):
# #         """Decrement attempts for batches that *need* a token."""
# #         for batch_keys in pending_batches:
# #             for k in batch_keys:
# #                 tracker[k]["attempts_left"] -= 1

# #     def _rebuild_waiting_batches(self, tracker, batch_size):
# #         """
# #         Re-chunk not-yet-sent requests (status waiting|processing) into
# #         fresh batches.  'queued' items stay out so we never republish them.
# #         """
# #         waiting_keys = [
# #             k for k, v in tracker.items()
# #             if v["status"] in ("waiting", "processing")
# #         ]
# #         return [
# #             waiting_keys[i : i + batch_size]
# #             for i in range(0, len(waiting_keys), batch_size)
# #         ]
