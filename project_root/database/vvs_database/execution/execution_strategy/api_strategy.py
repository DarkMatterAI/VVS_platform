from __future__ import annotations
import asyncio, math, random
from typing import Dict, List, Tuple

from vvs_database import logging
from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
from vvs_database.execution.execution_strategy.formatters import get_formatter
from vvs_database.execution.execution_strategy.state_models import (
    APIRequestState,
    StrategyResponse,
    APIConst,
)
from vvs_database.execution.execution_strategy.utils import _chunk
from vvs_database.execution.connections import Connections
from vvs_database.utils import make_post_request
from vvs_database.schemas import (
    ExecuteParams,
    ExecuteRequestUnion,
    ExecuteResponseUnion,
    ExecutionSources,
    PluginInDB,
)


class APIExecutionStrategy(ExecutionStrategy):
    """
    HTTP-POST execution respecting both in-process and distributed
    concurrency limits.
    """

    # ───────────────────────────────────────────────────────────────── #
    # life-cycle                                                      #
    # ───────────────────────────────────────────────────────────────── #
    def __init__(self, connections: Connections, params: ExecuteParams):
        super().__init__(connections, params)
        self.redis   = connections.redis_service
        self.params  = params
        self.log_id  = "APIExecute"

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

        const  = APIConst.from_plugin(plugin, self.params)
        fmt    = get_formatter(plugin)
        state  = self._init_state(requests)

        while state.pending:
            tokens = await self._acquire_tokens(const, state)
            if not await self._prepare_wave(tokens, state, const):
                continue
            await self._fire_wave(fmt, const, state)
            await self._release_tokens(tokens, const)

        return {s.key: s.response.as_legacy_dict() for s in state.done}

    # ───────────────────────────────────────────────────────────────── #
    # internal helpers                                                 #
    # ───────────────────────────────────────────────────────────────── #

    # ----- state bundles -------------------------------------------- #
    class _ExecState:
        def __init__(self, pending: List[APIRequestState]):
            self.pending: List[APIRequestState] = pending
            self.wave:    List[APIRequestState] = []
            self.done:    List[APIRequestState] = []
            self.tokens:  List[str]             = []

    # ----- build initial state -------------------------------------- #
    def _init_state(self, reqs: Dict[str, ExecuteRequestUnion]) -> "_ExecState":
        p = self.params
        pending = [
            APIRequestState(key=k, request=r, attempts_left=p.max_semaphore_attempts)
            for k, r in reqs.items()
        ]
        return self._ExecState(pending)

    # ----- semaphore handling --------------------------------------- #
    async def _acquire_tokens(
        self,
        const: APIConst,
        state: "_ExecState",
    ) -> List[str]:
        if not self.params.use_semaphore:
            return []

        need_batches = math.ceil(len(state.pending) / const.bs)
        need_tokens  = min(need_batches, const.max_conc)
        if need_tokens == 0:
            return []

        return await self.redis.acquire_semaphores_batch(
            const.sem_name,
            need_tokens,
            const.max_conc,
            lock_timeout=const.lock_t,
        )

    async def _release_tokens(self, tokens: List[str], const: APIConst) -> None:
        if tokens and self.params.use_semaphore:
            await self.redis.release_semaphore(const.sem_name, tokens)

    # ----- wave selection / back-off -------------------------------- #
    async def _prepare_wave(
        self,
        tokens: List[str],
        st: "_ExecState",
        const: APIConst,
    ) -> bool:
        """
        Returns **True** if a wave is ready, **False** when we had to
        back-off and the caller should re-enter the loop.
        """
        if self.params.use_semaphore and not tokens:
            sleep_time = self._handle_backoff(st, const)
            await asyncio.sleep(sleep_time)
            return False

        cap                    = (len(tokens) or const.max_conc) * const.bs
        st.wave, st.pending    = st.pending[:cap], st.pending[cap:]
        st.tokens              = tokens
        const.reset_backoff()
        return True

    def _handle_backoff(self, st: "_ExecState", const: APIConst) -> None:
        """Decrement attempts; convert exhausted requests to failures."""
        for req in st.pending:
            req.attempts_left -= 1
            if req.attempts_left <= 0:
                req.response = StrategyResponse.failure(
                    "Semaphore failure", "Exceeded max attempts"
                )
                st.done.append(req)
        st.pending = [r for r in st.pending if r.response is None]
        # async sleep for back-off
        sleep_time = const.backoff_sleep()
        return sleep_time 

    # ----- network round-trip --------------------------------------- #
    async def _fire_wave(
        self,
        fmt,
        const: APIConst,
        st: "_ExecState",
    ) -> None:
        sem     = asyncio.Semaphore(const.max_conc)
        batches = _chunk(st.wave, const.bs)

        async def _send(batch: List[APIRequestState]) -> None:
            payload = fmt.build_payload(batch, const.bs)
            try:
                raw = await make_post_request(
                    payload,
                    const.url,
                    const.timeout,
                    const.retries,
                    retry_sleep=1.0,
                    log_id=self.log_id,
                    verbose=False,
                )
                parsed = fmt.parse_response(raw, const.bs)
                for slot, req in enumerate(batch):
                    req.response = StrategyResponse(
                        valid=True,
                        response_data=parsed[slot],
                        source=ExecutionSources.EXECUTION,
                    )
            except Exception as exc:
                logging.error("%s: POST failed – %s", self.log_id, exc)
                for req in batch:
                    req.response = StrategyResponse.failure("HTTP failure", str(exc))

        async def _bounded(batch: List[APIRequestState]) -> None:
            async with sem:
                await _send(batch)

        await asyncio.gather(*[_bounded(b) for b in batches])
        st.done.extend(st.wave)
        st.wave = []


# from __future__ import annotations
# import asyncio, math, random
# from typing import Dict, List

# from vvs_database import logging
# from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
# from vvs_database.execution.execution_strategy.formatters import get_formatter
# from vvs_database.execution.execution_strategy.state_models import (
#     APIRequestState,
#     StrategyResponse,
#     APIConst
# )
# from vvs_database.execution.execution_strategy.utils import _chunk
# from vvs_database.execution.connections import Connections
# from vvs_database.utils import make_post_request
# from vvs_database.schemas import (
#     ExecuteParams,
#     ExecuteRequestUnion,
#     ExecuteResponseUnion,
#     ExecutionSources,
#     PluginInDB,
# )


# class APIExecutionStrategy(ExecutionStrategy):
#     """
#     HTTP-POST execution respecting both local and distributed
#     concurrency limits.
#     """

#     # ------------------------------------------------------------------ #
#     # life-cycle                                                         #
#     # ------------------------------------------------------------------ #

#     def __init__(self, connections: Connections, params: ExecuteParams):
#         super().__init__(connections, params)
#         self.redis   = connections.redis_service
#         self.params  = params
#         self.log_id  = "APIExecute"

#     # ------------------------------------------------------------------ #
#     # public API                                                         #
#     # ------------------------------------------------------------------ #

#     async def execute(
#         self,
#         plugin:   PluginInDB,
#         requests: Dict[str, ExecuteRequestUnion],
#     ) -> Dict[str, ExecuteResponseUnion]:

#         if not requests:
#             return {}

#         self._init_constants(plugin)                         # sets self.*
#         fmt   = get_formatter(plugin)
#         state = self._init_state(requests)

#         while state.pending:                                  # main loop
#             tokens = await self._maybe_acquire_tokens(plugin, state)
#             if not self._prep_wave(tokens, state):
#                 continue                                      # loop handled back-off

#             await self._fire_wave(fmt, plugin, state)         # HTTP round-trip
#             await self._maybe_release_tokens(plugin, tokens)

#         return {s.key: s.response.as_legacy_dict() for s in state.done}

#     # ------------------------------------------------------------------ #
#     # helpers                                                            #
#     # ------------------------------------------------------------------ #

#     # ----- init & constant caching ------------------------------------ #
#     def _init_constants(self, plugin: PluginInDB):
#         p               = self.params
#         self._url       = plugin.endpoint_url
#         self._timeout   = plugin.timeout
#         self._retries   = plugin.max_retries
#         self._bs        = plugin.batch_size
#         self._max_conc  = plugin.max_concurrency
#         self._sem_name  = f"plugin:{plugin.id}"
#         self._lock_t    = int(self._timeout * 1.1)
#         self._back_base = self._timeout / self._max_conc
#         self._back_cur  = self._back_base
#         self._back_fac  = p.backoff_factor

#     class _ExecState:
#         def __init__(self, pend: List[APIRequestState]):
#             self.pending = pend
#             self.done: List[APIRequestState] = []
#             self.wave: List[APIRequestState] = []
#             self.tokens: List[str] = []

#     def _init_state(self, reqs: Dict[str, ExecuteRequestUnion]) -> "_ExecState":
#         p = self.params
#         pend = [
#             APIRequestState(key=k, request=r, attempts_left=p.max_semaphore_attempts)
#             for k, r in reqs.items()
#         ]
#         return self._ExecState(pend)

#     # ----- token handling --------------------------------------------- #
#     async def _maybe_acquire_tokens(self, plugin: PluginInDB, st: "_ExecState") -> List[str]:
#         if not self.params.use_semaphore:
#             return []                                           # dummy ⇒ no limit here

#         need = min(len(st.pending), self._max_conc * self._bs)     # upper bound
#         need = math.ceil(need / self._bs)                          # batches → tokens
#         if not need:
#             return []

#         return await self.redis.acquire_semaphores_batch(
#             self._sem_name, need, self._max_conc, lock_timeout=self._lock_t
#         )

#     async def _maybe_release_tokens(self, plugin: PluginInDB, tokens: List[str]):
#         if tokens and self.params.use_semaphore:
#             await self.redis.release_semaphore(self._sem_name, tokens)

#     # ----- wave selection & back-off ---------------------------------- #
#     def _prep_wave(self, tokens: List[str], st: "_ExecState") -> bool:
#         """
#         Build `st.wave` from `st.pending`.
#         Returns True  if we actually have a wave to fire,
#                 False if we had to back-off (no tokens etc.).
#         """
#         if self.params.use_semaphore and not tokens:
#             # no tokens – back-off
#             self._decrement_or_fail(st)
#             self._sleep_backoff()
#             return False

#         cap        = (len(tokens) or self._max_conc) * self._bs
#         st.wave, st.pending = st.pending[:cap], st.pending[cap:]
#         st.tokens  = tokens
#         self._back_cur = self._back_base
#         return True

#     def _decrement_or_fail(self, st: "_ExecState"):
#         for req in st.pending:
#             req.attempts_left -= 1
#             if req.attempts_left <= 0:
#                 req.response = StrategyResponse.failure("Semaphore failure", "Exceeded max attempts")
#                 st.done.append(req)
#         st.pending = [r for r in st.pending if r.response is None]

#     def _sleep_backoff(self):
#         jitter = 0.8 + random.random() * 0.4
#         time = self._back_cur * jitter
#         self._back_cur = min(self._back_cur * self._back_fac, self._timeout)
#         return asyncio.sleep(time)

#     # ----- network I/O ------------------------------------------------- #
#     async def _fire_wave(self, fmt, plugin, st: "_ExecState"):
#         sem = asyncio.Semaphore(self._max_conc)
#         batches = _chunk(st.wave, self._bs)

#         async def _send(batch):
#             payload = fmt.build_payload(batch, self._bs)
#             try:
#                 raw = await make_post_request(
#                     payload, self._url, self._timeout, self._retries,
#                     retry_sleep=1.0, log_id=self.log_id, verbose=False
#                 )
#                 parsed = fmt.parse_response(raw, self._bs)
#                 for slot, req in enumerate(batch):
#                     req.response = StrategyResponse(
#                         valid=True,
#                         response_data=parsed[slot],
#                         source=ExecutionSources.EXECUTION,
#                     )
#             except Exception as exc:
#                 logging.error("%s: POST failed - %s", self.log_id, exc)
#                 for req in batch:
#                     req.response = StrategyResponse.failure("HTTP failure", str(exc))

#         async def _bounded(batch):
#             async with sem:     # local in-process guard
#                 await _send(batch)

#         await asyncio.gather(*[_bounded(b) for b in batches])
#         st.done.extend(st.wave)
#         st.wave = []







# from __future__ import annotations
# import asyncio, math, random
# from typing import Dict, List

# from vvs_database import logging
# from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
# from vvs_database.execution.execution_strategy.formatters import get_formatter
# from vvs_database.execution.execution_strategy.state_models import (
#     APIRequestState,
#     StrategyResponse,
# )
# from vvs_database.utils import make_post_request
# from vvs_database.execution.connections import Connections
# from vvs_database.execution.execution_strategy.utils import _chunk
# from vvs_database.schemas import (
#     ExecuteParams,
#     ExecuteRequestUnion,
#     ExecuteResponseUnion,
#     ExecutionSources,
#     PluginInDB,
# )

# # ─────────────────────────────────────────────────────────────────────────────

# class APIExecutionStrategy(ExecutionStrategy):
#     """
#     HTTP POST execution respecting both in-process and distributed
#     (Redis semaphore) concurrency limits.
#     """

#     def __init__(self, connections: Connections, params: ExecuteParams):
#         super().__init__(connections, params)
#         self.redis   = connections.redis_service
#         self.params  = params
#         self.log_id  = "APIExecute"

#     # --------------------------------------------------------------------- #

#     async def execute(
#         self,
#         plugin: PluginInDB,
#         requests: Dict[str, ExecuteRequestUnion],
#     ) -> Dict[str, ExecuteResponseUnion]:

#         if not requests:
#             return {}

#         fmt         = get_formatter(plugin)
#         p           = self.params
#         url         = plugin.endpoint_url
#         timeout     = plugin.timeout
#         lock_t      = int(timeout * 1.1)
#         bs          = plugin.batch_size
#         max_conc    = plugin.max_concurrency
#         sem_name    = f"plugin:{plugin.id}"
#         back_factor = p.backoff_factor
#         backoff_cur = timeout / max_conc

#         # ----- initialise request state ----------------------------------
#         pending: List[APIRequestState] = [
#             APIRequestState(
#                 key=k,
#                 request=req,
#                 attempts_left=p.max_semaphore_attempts,
#             )
#             for k, req in requests.items()
#         ]
#         done: List[APIRequestState] = []

#         # ----- main loop --------------------------------------------------
#         while pending:

#             # 1) try to grab semaphore tokens
#             want = min(max_conc, math.ceil(len(pending) / bs))
#             tokens: List[str] = []
#             if p.use_semaphore:
#                 tokens = await self.redis.acquire_semaphores_batch(
#                     sem_name, want, max_conc, lock_timeout=lock_t
#                 )

#             if p.use_semaphore and not tokens:
#                 # back-off round
#                 for st in pending:
#                     st.attempts_left -= 1
#                     if st.attempts_left <= 0:
#                         st.response = StrategyResponse.failure(
#                             "Semaphore failure", "Exceeded max attempts"
#                         )
#                         done.append(st)
#                 pending = [st for st in pending if st.response is None]
#                 await asyncio.sleep(backoff_cur * (0.8 + random.random() * 0.4))
#                 backoff_cur = min(backoff_cur * back_factor, timeout)
#                 continue

#             backoff_cur = timeout / max_conc  # reset after progress

#             # 2) build the wave we *will* execute
#             cap   = (len(tokens) or want) * bs
#             wave, pending = pending[:cap], pending[cap:]
#             batches = _chunk(wave, bs)
#             sem     = asyncio.Semaphore(max_conc)

#             # 3) fire the HTTP POSTs
#             async def _fire(batch: List[APIRequestState]):
#                 payload = fmt.build_payload(batch, bs)
#                 try:
#                     resp_raw = await make_post_request(
#                         payload,
#                         url,
#                         timeout,
#                         plugin.max_retries,
#                         retry_sleep=1.0,
#                         log_id=self.log_id,
#                         verbose=False,
#                     )
#                     parsed = fmt.parse_response(resp_raw, bs)
#                     for slot, st in enumerate(batch):
#                         st.response = StrategyResponse(
#                             valid=True,
#                             response_data=parsed[slot],
#                             source=ExecutionSources.EXECUTION,
#                         )
#                 except Exception as exc:
#                     logging.error("%s: POST failed - %s", self.log_id, exc)
#                     for st in batch:
#                         st.response = StrategyResponse.failure(
#                             "HTTP failure", str(exc)
#                         )

#             async def _bounded(coro):
#                 async with sem:
#                     await coro

#             await asyncio.gather(*[_bounded(_fire(b)) for b in batches])
#             done.extend(wave)

#             # 4) release tokens
#             if tokens and p.use_semaphore:
#                 await self.redis.release_semaphore(sem_name, tokens)

#         # ----- aggregate into legacy shape --------------------------------
#         return {st.key: st.response.as_legacy_dict() for st in done}













# # from __future__ import annotations

# # import asyncio
# # import math
# # import random
# # from typing import Dict, List, Tuple

# # from vvs_database import logging
# # from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
# # from vvs_database.execution.execution_strategy.formatters import get_formatter
# # from vvs_database.schemas import (
# #     ExecuteParams,
# #     ExecuteRequestUnion,
# #     ExecuteResponseUnion,
# #     ExecutionSources,
# #     PluginInDB,
# # )
# # from vvs_database.utils import make_post_request
# # from vvs_database.execution.connections import Connections


# # # --------------------------------------------------------------------------- #
# # #  internal helpers                                                           #
# # # --------------------------------------------------------------------------- #

# # async def _bounded(semaphore: asyncio.Semaphore, coro):
# #     async with semaphore:
# #         return await coro


# # def _batch(lst: List, n: int) -> List[List]:
# #     """Chunk *lst* into len≤n slices (last may be shorter)."""
# #     return [lst[i : i + n] for i in range(0, len(lst), n)]


# # def _failure(reason: str, detail: str) -> Dict:
# #     return {
# #         "valid": False,
# #         "response_data": None,
# #         "failure_reason": reason,
# #         "failure_detail": detail,
# #         "source": ExecutionSources.FAILURE,
# #     }


# # # --------------------------------------------------------------------------- #
# # #  strategy                                                                   #
# # # --------------------------------------------------------------------------- #

# # class APIExecutionStrategy(ExecutionStrategy):
# #     """
# #     Fire HTTP-POST batches against the remote plugin endpoint, observing
# #     both in-process and distributed (Redis) concurrency limits.
# #     """

# #     # -------- life-cycle ----------------------------------------------------

# #     def __init__(self, connections: Connections, execute_params: ExecuteParams):
# #         super().__init__(connections, execute_params)
# #         self.redis  = connections.redis_service
# #         self.params = execute_params
# #         self.log_id = "APIExecute"

# #     # -------- public API ----------------------------------------------------

# #     async def execute(
# #         self,
# #         plugin: PluginInDB,
# #         requests: Dict[str, ExecuteRequestUnion],
# #     ) -> Dict[str, ExecuteResponseUnion]:

# #         if not requests:
# #             return {}

# #         # ---- constant knobs ------------------------------------------------
# #         url             = plugin.endpoint_url
# #         timeout         = plugin.timeout
# #         lock_timeout    = int(timeout * 1.1)
# #         retries         = plugin.max_retries
# #         bs              = plugin.batch_size
# #         max_conc        = plugin.max_concurrency
# #         semaphore_name  = f"plugin:{plugin.id}"
# #         backoff_factor  = self.params.backoff_factor
# #         cur_backoff     = timeout / max_conc
# #         formatter       = get_formatter(plugin)

# #         # ---- working lists -------------------------------------------------
# #         pending: List[Dict] = [
# #             {"key": k, "request": r, "attempts_left": self.params.max_semaphore_attempts}
# #             for k, r in requests.items()
# #         ]
# #         completed: List[Dict] = []

# #         # ---- main loop -----------------------------------------------------
# #         while pending:

# #             # 1) acquire semaphore tokens (optional) ---------------------
# #             need_tokens = min(max_conc, math.ceil(len(pending) / bs))
# #             tokens: List[str] = []
# #             if self.params.use_semaphore:
# #                 tokens = await self.redis.acquire_semaphores_batch(
# #                     semaphore_name, need_tokens, max_conc, lock_timeout=lock_timeout
# #                 )

# #             if not tokens and self.params.use_semaphore:
# #                 # back-off: nothing available this round
# #                 logging.debug("%s: no semaphore tokens, backing off", self.log_id)
# #                 for entry in pending:
# #                     entry["attempts_left"] -= 1
# #                     if entry["attempts_left"] <= 0:
# #                         entry["response"] = _failure("Semaphore failure", "Exceeded max attempts")
# #                         completed.append(entry)
# #                 pending = [e for e in pending if "response" not in e]
# #                 jitter = 0.8 + random.random() * 0.4
# #                 await asyncio.sleep(cur_backoff * jitter)
# #                 cur_backoff = min(cur_backoff * backoff_factor, timeout)
# #                 continue

# #             cur_backoff = timeout / max_conc  # reset after progress

# #             # 2) pick the wave we *will* execute -------------------------
# #             wave_cap   = (len(tokens) or need_tokens) * bs
# #             wave, pending = pending[:wave_cap], pending[wave_cap:]

# #             # 3) split wave into batches & fire them --------------------
# #             batches = _batch(wave, bs)
# #             sem     = asyncio.Semaphore(max_conc)

# #             async def _fire(batch: List[Dict]):
# #                 payload = formatter.build_payload(batch, bs)
# #                 try:
# #                     http_resp = await make_post_request(
# #                         payload,
# #                         url,
# #                         timeout,
# #                         retries,
# #                         retry_sleep=1.0,
# #                         log_id=self.log_id,
# #                         verbose=False,
# #                     )
# #                     parsed = formatter.parse_response(http_resp, bs)
# #                     for slot, ent in enumerate(batch):
# #                         ent["response"] = {
# #                             "valid": True,
# #                             "response_data": parsed[slot],
# #                             "source": ExecutionSources.EXECUTION,
# #                         }
# #                 except Exception as exc:
# #                     logging.error("%s: POST to %s failed - %s", self.log_id, url, exc)
# #                     for ent in batch:
# #                         ent["response"] = _failure("Post request failure", str(exc))

# #             await asyncio.gather(*[_bounded(sem, _fire(b)) for b in batches])

# #             completed.extend(wave)

# #             # 4) release distributed tokens -----------------------------
# #             if tokens and self.params.use_semaphore:
# #                 await self.redis.release_semaphore(semaphore_name, tokens)

# #         # ---- aggregate ------------------------------------------------------
# #         return {e["key"]: e["response"] for e in completed}


# # # import asyncio 
# # # import random 
# # # import math 
# # # from typing import Dict, List, Tuple  

# # # from vvs_database.schemas import (
# # #     ExecuteRequestUnion, 
# # #     ExecuteResponseUnion, 
# # #     ExecuteParams,
# # #     PluginInDB,
# # #     ExecutionSources
# # # )
# # # from vvs_database.utils import make_post_request
# # # from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
# # # from vvs_database.execution.connections import Connections
# # # from vvs_database.execution.execution_strategy.formatters import get_formatter
# # # from vvs_database import logging

# # # async def concurrency_bounded_func(semaphore, func, input, kwargs):
# # #     """Run function within concurrency limit."""
# # #     async with semaphore:
# # #         output = await func(input, **kwargs)
# # #     return output

# # # async def concurrency_wrapper(concurrency, func, iterable, kwargs):
# # #     """Control in-process concurrency"""
# # #     semaphore = asyncio.Semaphore(concurrency)
    
# # #     tasks = [concurrency_bounded_func(semaphore, func, item, kwargs) for item in iterable]
# # #     results = await asyncio.gather(*tasks)
# # #     return results

# # # class APIExecutionStrategy(ExecutionStrategy):
# # #     """Strategy for executing API-based plugins"""
# # #     def __init__(self, 
# # #                  connections: Connections,
# # #                  execute_params: ExecuteParams,
# # #                  ):
# # #         self.redis_service = connections.redis_service 
# # #         self.execute_params = execute_params
# # #         self.log_id = 'API Execute'

# # #     def batch_requests(self, 
# # #                        request_list: List[Tuple[str, ExecuteRequestUnion]],
# # #                        batch_size: int
# # #                        ):
# # #         batches = [request_list[i:i+batch_size] 
# # #                    for i in range(0, len(request_list), batch_size)]
# # #         return batches 
    
# # #     def _add_failure_result(self, batch, failure_reason, failure_detail):
# # #         failure_result = {"valid": False, 
# # #                           "response_data": None, 
# # #                           "failure_reason": failure_reason, 
# # #                           "failure_detail": failure_detail,
# # #                           "source": ExecutionSources.FAILURE}
# # #         for request in batch:
# # #             request["response"] = failure_result

# # #     async def execute(
# # #         self,
# # #         plugin: PluginInDB,
# # #         requests: Dict[str, ExecuteRequestUnion],
# # #     ) -> Dict[str, ExecuteResponseUnion]:
# # #         logging.info(f"{self.log_id}: Executing %d requests", len(requests))
# # #         if not requests:
# # #             return {}

# # #         # ───────── plugin‑level params ────────────────────────────────────
# # #         url              = plugin.endpoint_url
# # #         timeout          = plugin.timeout
# # #         lock_timeout     = int(1.1 * timeout)
# # #         retries          = plugin.max_retries
# # #         batch_size       = plugin.batch_size
# # #         max_concurrency  = plugin.max_concurrency
# # #         semaphore_name   = f"plugin:{plugin.id}"
# # #         initial_backoff  = timeout / max_concurrency  # first guess
# # #         backoff_factor   = self.execute_params.backoff_factor
# # #         log_id           = self.log_id
# # #         formatter        = get_formatter(plugin)

# # #         # ───────── helper: fire N batches concurrently ────────────────────
# # #         async def process_batch(batch):
# # #             payload = formatter.build_payload(batch, batch_size)
# # #             try:
# # #                 response = await make_post_request(
# # #                     payload,
# # #                     url,
# # #                     timeout,
# # #                     retries,
# # #                     retry_sleep=1.0,
# # #                     log_id=log_id,
# # #                     verbose=False
# # #                 )
# # #                 response = formatter.parse_response(response, batch_size)
# # #                 for slot, b in enumerate(batch):
# # #                     b["response"] = {
# # #                         "valid": True,
# # #                         "response_data": response[slot],
# # #                         "source": ExecutionSources.EXECUTION
# # #                     }
# # #             except Exception as e:                      # network failure
# # #                 logging.error("%s: POST to %s failed - %s", log_id, url, e)
# # #                 self._add_failure_result(batch, "Post request failure", str(e))
# # #             return batch 


# # #         req_list = [{"key": k, 
# # #                      "request": r, 
# # #                      "attempts_left": self.execute_params.max_semaphore_attempts}
# # #                      for k,r in requests.items()]

# # #         # ───────── run waves until exhausted ──────────────────────────────
# # #         all_results: list[dict] = []
# # #         current_backoff = initial_backoff

# # #         while req_list:
# # #             n_needed = min(max_concurrency, math.ceil(len(req_list)/batch_size))
# # #             identifiers = []
# # #             if self.execute_params.use_semaphore:
# # #                 identifiers = await self.redis_service.acquire_semaphores_batch(
# # #                     name=semaphore_name,
# # #                     n=n_needed,
# # #                     max_locks=max_concurrency,
# # #                     lock_timeout=lock_timeout,
# # #                 )

# # #             # ------------------ No tokens this round ----------------------
# # #             if self.execute_params.use_semaphore and not identifiers:
# # #                 # decrement attempts for *every* still‑waiting request
# # #                 for entry in req_list:
# # #                     entry["attempts_left"] -= 1

# # #                 # harvest the ones that ran out of attempts
# # #                 timed_out, still_waiting = [], []
# # #                 for entry in req_list:
# # #                     (timed_out if entry["attempts_left"] <= 0 else still_waiting).append(entry)
# # #                 req_list = still_waiting
# # #                 self._add_failure_result(timed_out, "Semaphore failure", "Exceeded max attempts")
# # #                 all_results.extend(timed_out)

# # #                 # back‑off then continue
# # #                 jitter = 0.8 + (random.random() * 0.4)
# # #                 await asyncio.sleep(current_backoff * jitter)
# # #                 current_backoff = min(current_backoff * backoff_factor, timeout)
# # #                 continue  # retry loop

# # #             # ------------------ Have some tokens --------------------------
# # #             grab = len(identifiers) if identifiers else n_needed
# # #             grab = grab * batch_size 
# # #             wave_entries, req_list = req_list[:grab], req_list[grab:]

# # #             # entries left in pending_batches lost this round → attempts‑1
# # #             for entry in req_list:
# # #                 entry["attempts_left"] -= 1
# # #             # cull newly exhausted ones
# # #             exhausted, still_waiting = [], []
# # #             for e in req_list:
# # #                 (exhausted if e["attempts_left"] <= 0 else still_waiting).append(e)
# # #             req_list = still_waiting
# # #             self._add_failure_result(exhausted, "Semaphore failure", "Exceeded max attempts")
# # #             all_results.extend(exhausted)

# # #             # run the wave we actually got tokens for
# # #             wave = self.batch_requests(wave_entries, batch_size)
# # #             wave_results = await concurrency_wrapper(
# # #                 max_concurrency, process_batch, wave, {}
# # #             )
# # #             for wr in wave_results:
# # #                 all_results.extend(wr)

# # #             if self.execute_params.use_semaphore and identifiers:
# # #                 await self.redis_service.release_semaphore(semaphore_name, identifiers)

# # #             current_backoff = initial_backoff  # reset after a productive wave

# # #         # ───────── final aggregation → {key: ExecuteResponseUnion} ────────
# # #         return {r["key"]: r["response"] for r in all_results}



# # #     # async def execute(
# # #     #     self,
# # #     #     plugin: PluginInDB,
# # #     #     requests: Dict[str, ExecuteRequestUnion],
# # #     # ) -> Dict[str, ExecuteResponseUnion]:
# # #     #     logging.info(f"{self.log_id}: Executing %d requests", len(requests))
# # #     #     if not requests:
# # #     #         return {}

# # #     #     # ───────── plugin‑level params ────────────────────────────────────
# # #     #     url              = plugin.endpoint_url
# # #     #     timeout          = plugin.timeout
# # #     #     lock_timeout     = int(1.1 * timeout)
# # #     #     retries          = plugin.max_retries
# # #     #     batch_size       = plugin.batch_size
# # #     #     max_concurrency  = plugin.max_concurrency
# # #     #     semaphore_name   = f"plugin:{plugin.id}"
# # #     #     initial_backoff  = timeout / max_concurrency  # first guess
# # #     #     backoff_factor   = self.execute_params.backoff_factor
# # #     #     log_id           = self.log_id
# # #     #     formatter        = get_formatter(plugin)

# # #     #     # ───────── build request‑batches *once* ───────────────────────────
# # #     #     req_list = [
# # #     #         {"key": k, "request": r} for k, r in requests.items()
# # #     #     ]
# # #     #     request_batches = self.batch_requests(req_list, batch_size)
# # #     #     pending_batches = request_batches[:]          # shallow copy

# # #     #     # ───────── helper: fire N batches concurrently ────────────────────
# # #     #     async def process_batch(batch):
# # #     #         payload = formatter.build_payload(batch, batch_size)
# # #     #         try:
# # #     #             response = await make_post_request(
# # #     #                 payload,
# # #     #                 url,
# # #     #                 timeout,
# # #     #                 retries,
# # #     #                 retry_sleep=1.0,
# # #     #                 log_id=log_id,
# # #     #                 verbose=False
# # #     #             )
# # #     #             response = formatter.parse_response(response, batch_size)
# # #     #             for slot, b in enumerate(batch):
# # #     #                 b["response"] = {
# # #     #                     "valid": True,
# # #     #                     "response_data": response[slot],
# # #     #                 }
# # #     #         except Exception as e:                      # network failure
# # #     #             logging.error("%s: POST to %s failed - %s", log_id, url, e)
# # #     #             self._add_failure_result(batch, "Post request failure", str(e))
# # #     #         return batch 

# # #     #     # ------------------------------------------------------------------
# # #     #     # build pending_batches :: list[ dict(batch=..., attempts_left=int) ]
# # #     #     # ------------------------------------------------------------------
# # #     #     pending_batches = [
# # #     #         {"batch": b, "attempts_left": self.execute_params.max_semaphore_attempts}
# # #     #         for b in request_batches
# # #     #     ]

# # #     #     # ───────── run waves until exhausted ──────────────────────────────
# # #     #     all_results: list[dict] = []
# # #     #     current_backoff = initial_backoff

# # #     #     while pending_batches:
# # #     #         n_needed = min(len(pending_batches), max_concurrency)
# # #     #         identifiers = []
# # #     #         if self.execute_params.use_semaphore:
# # #     #             identifiers = await self.redis_service.acquire_semaphores_batch(
# # #     #                 name=semaphore_name,
# # #     #                 n=n_needed,
# # #     #                 max_locks=max_concurrency,
# # #     #                 lock_timeout=lock_timeout,
# # #     #             )

# # #     #         # ------------------ No tokens this round ----------------------
# # #     #         if self.execute_params.use_semaphore and not identifiers:
# # #     #             # decrement attempts for *every* still‑waiting batch
# # #     #             for entry in pending_batches:
# # #     #                 entry["attempts_left"] -= 1

# # #     #             # harvest the ones that ran out of attempts
# # #     #             timed_out, still_waiting = [], []
# # #     #             for entry in pending_batches:
# # #     #                 (timed_out if entry["attempts_left"] <= 0 else still_waiting).append(entry)
# # #     #             pending_batches = still_waiting
# # #     #             for entry in timed_out:
# # #     #                 self._add_failure_result(entry["batch"], "Semaphore failure",
# # #     #                                          "Exceeded max attempts")
# # #     #                 all_results.extend(entry["batch"])

# # #     #             # back‑off then continue
# # #     #             jitter = 0.8 + (random.random() * 0.4)
# # #     #             await asyncio.sleep(current_backoff * jitter)
# # #     #             current_backoff = min(current_backoff * backoff_factor, timeout)
# # #     #             continue  # retry loop

# # #     #         # ------------------ Have some tokens --------------------------
# # #     #         grab = len(identifiers) if identifiers else n_needed
# # #     #         wave_entries, pending_batches = pending_batches[:grab], pending_batches[grab:]

# # #     #         # entries left in pending_batches lost this round → attempts‑1
# # #     #         for entry in pending_batches:
# # #     #             entry["attempts_left"] -= 1
# # #     #         # cull newly exhausted ones
# # #     #         exhausted, still_waiting = [], []
# # #     #         for e in pending_batches:
# # #     #             (exhausted if e["attempts_left"] <= 0 else still_waiting).append(e)
# # #     #         pending_batches = still_waiting
# # #     #         for ex in exhausted:
# # #     #             self._add_failure_result(ex["batch"], "Semaphore failure",
# # #     #                                      "Exceeded max attempts")
# # #     #             all_results.extend(ex["batch"])

# # #     #         # run the wave we actually got tokens for
# # #     #         wave = [e["batch"] for e in wave_entries]
# # #     #         wave_results = await concurrency_wrapper(
# # #     #             max_concurrency, process_batch, wave, {}
# # #     #         )
# # #     #         for wr in wave_results:
# # #     #             all_results.extend(wr)
# # #     #             # all_results.extend(wr if isinstance(wr, list) else [wr])

# # #     #         if self.execute_params.use_semaphore and identifiers:
# # #     #             await self.redis_service.release_semaphore(semaphore_name, identifiers)

# # #     #         current_backoff = initial_backoff  # reset after a productive wave

# # #     #     # ───────── final aggregation → {key: ExecuteResponseUnion} ────────
# # #     #     return {r["key"]: r["response"] for r in all_results}
