from __future__ import annotations
import asyncio, time, random
from typing import Dict, List, Tuple, Type, Any 

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
    def __init__(self, 
                 connections: Connections, 
                 params: ExecuteParams,
                 response_model: Type):
        super().__init__(connections, params)
        self.redis   = connections.redis_service
        self.rabbit  = connections.rabbitmq_service
        self.params  = params
        self.log_id  = "QueueExecute"
        self._resp_model = response_model

    # ───────────────────────────────────────────────────────────────── #
    # public API                                                        #
    # ───────────────────────────────────────────────────────────────── #
    async def execute(
        self,
        plugin:   PluginInDB,
        requests: Dict[str, ExecuteRequestUnion],
    ) -> Dict[str, StrategyResponse]:

        if not requests:
            return {}

        const    = QueueConst.from_plugin(plugin, self.params)
        tracker  = self._init_tracker(requests, const)

        while self._unfinished(tracker):
            await self._consume_cache(tracker, const)

            pending_batches = self._waiting_batches(tracker, const.bs)

            tokens = await self._acquire_tokens(pending_batches, const)
            if await self._maybe_backoff(tokens, pending_batches, const, tracker):
                continue

            # 1. pick & publish -------------------------------------------------
            wave_batches, token_list = self._pick_wave(pending_batches, tokens, const)
            self._mark_processing(wave_batches, token_list, tracker)
            if wave_batches:
                logging.info("Publishing %s batches", len(wave_batches))
            await asyncio.gather(
                *[self._publish_batch(b, tracker, const.bs) for b in wave_batches]
            )

            # 2. wait / collect results ----------------------------------------
            await self._poll_results(tracker, const)
            self._apply_timeouts(tracker, const)

            # 3. figure out which tokens are now free --------------------------
            await self._release_tokens(tracker, const)

            await asyncio.sleep(const.poll_interval)

            # wave_batches, token_list = self._pick_wave(pending_batches, tokens, const)
            # self._mark_processing(wave_batches, token_list, tracker)
            # if len(wave_batches)>0:
            #     logging.info(f"Publishing {len(wave_batches)} batches")
            # await asyncio.gather(
            #     *[self._publish_batch(b, tracker, plugin) for b in wave_batches]
            # )
            # await self._release_tokens(tokens, const)

            # await self._poll_results(tracker, const)
            # self._apply_timeouts(tracker, const)
            # await asyncio.sleep(const.poll_interval)

        return {k: st.response for k, st in tracker.items()}

    # ───────────────────────────────────────────────────────────────── #
    # helper sections                                                   #
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
    
    async def _release_tokens(self, tracker: dict[str, QueueRequestState],
                            const: QueueConst) -> None:
        if not const.use_sema:
            return

        done   = [st for st in tracker.values()
                if st.status in ("done", "error") and st.identifier]
        tokens = list({st.identifier for st in done})     # unique

        if tokens:
            logging.info("Releasing %d semaphore tokens", len(tokens))
            await self.redis.release_semaphore(const.sem_name, tokens)
            for st in done:
                st.identifier = None
    
    # async def _release_tokens(self, tracker: Dict[str, QueueRequestState], const: QueueConst) -> None:
    #     done_tokens = []
    #     done_requests = []
    #     for st in tracker.values():
    #         if st.status in ("done", "error") and st.identifier:
    #             done_tokens.append(st.identifier)
    #             done_requests.append(st)

    #     if done_tokens and const.use_sema:
    #         done_tokens = list(set(done_tokens))
    #         logging.info(f"Releasing {len(done_tokens)} semaphores")
    #         await self.redis.release_semaphore(const.sem_name, done_tokens)

    #     for st in done_requests:
    #         st.identifier = None 

    # async def _release_tokens(self, tokens: List[str], const: QueueConst) -> None:
    #     if tokens and const.use_sema:
    #         await self.redis.release_semaphore(const.sem_name, tokens)

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
            logging.info("Semaphore backoff")
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

    # ---------- aggressive cache ------------------------------------ #
    async def _consume_cache(
        self,
        tracker: Dict[str, QueueRequestState],
        const: QueueConst,
    ) -> None:
        if not (self.params.cache or self.params.aggressive_cache):
            return
        keys = [
            k for k, st in tracker.items()
            if st.status in ("waiting", "processing")
        ]
        if not keys:
            return
        hits = await self.redis.get_results(keys)
        for k, payload in hits.items():
            st = tracker[k]
            st.status   = "done"
                # note - here we _dont_ unpack payload because cache
                # returns nested just resposne data
            st.response = StrategyResponse(
                valid=True,
                response_data=payload,
                source=ExecutionSources.CACHE,
            )

    async def _write_cache(self, updates: Dict[str, QueueRequestState]) -> None:
        if not (self.params.cache or self.params.aggressive_cache):
            return
        to_cache: Dict[str, Any] = {}
        for k, st in updates.items():
            if st.response and st.response.valid:
                try:
                    model = self._resp_model.model_validate(st.response.response_data)
                    to_cache[k] = model
                except:
                    # validation failures are handled by BaseExecutor
                    pass 
        if to_cache:
            await self.redis.set_results(to_cache)

    # ----- rabbit/redis ops ----------------------------------------- #
    async def _publish_batch(
        self,
        keys: List[str],
        tracker: Dict[str, QueueRequestState],
        batch_size: int
        # plugin: PluginInDB,
    ) -> None:
        msgs = [tracker[k].request for k in keys]
        try:
            sent = self.rabbit.publish_messages(msgs)
        except Exception as exc:
            logging.error("%s: RabbitMQ publish failed - %s", self.log_id, exc)
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
        freshly_done: Dict[str, QueueRequestState] = {}

        for rid, payload in hits.items():
            st = id_map[rid]
            st.status   = "done"
            st.response = StrategyResponse(
                # note - here we unpack payload because message consumer 
                # returns nested {"valid": True, "response_data": {}}
                **payload,
                source=ExecutionSources.EXECUTION,
            )
            freshly_done[st.key] = st

        await self._write_cache(freshly_done)

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

