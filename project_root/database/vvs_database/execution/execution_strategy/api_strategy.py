from __future__ import annotations
import asyncio, math, random
from typing import Dict, List, Tuple, Type, Any

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
    def __init__(self, 
                 connections: Connections, 
                 params: ExecuteParams,
                 response_model: Type):
        super().__init__(connections, params)
        self.redis   = connections.redis_service
        self.params  = params
        self.log_id  = "APIExecute"
        self._resp_model = response_model

    # ───────────────────────────────────────────────────────────────── #
    # public API                                                      #
    # ───────────────────────────────────────────────────────────────── #
    async def execute(
        self,
        plugin:   PluginInDB,
        requests: Dict[str, ExecuteRequestUnion],
    ) -> Dict[str, StrategyResponse]:

        if not requests:
            return {}

        const  = APIConst.from_plugin(plugin, self.params)
        fmt    = get_formatter(plugin)
        state  = self._init_state(requests)

        while state.pending:
            await self._consume_cache(state)
            
            tokens = await self._acquire_tokens(const, state)
            if not await self._prepare_wave(tokens, state, const):
                continue
            await self._fire_wave(fmt, const, state)
            await self._release_tokens(tokens, const)

        return {s.key: s.response for s in state.done}

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
    
    # ---------- hit Redis while requests still pending -------------- #
    async def _consume_cache(
        self,
        state: "_ExecState",
    ) -> None:
        if not self.params.aggressive_cache:
            return
        keys = [r.key for r in state.pending]
        if not keys:
            return
        hits = await self.redis.get_results(keys)
        if not hits:
            return

        hit_set = set(hits)
        new_done = []
        for req in state.pending:
            if req.key in hit_set:
                req.response = StrategyResponse(
                    valid=True,
                    response_data=hits[req.key],
                    source=ExecutionSources.CACHE,
                )
                new_done.append(req)

        # prune & stash
        state.pending = [r for r in state.pending if r.key not in hit_set]
        state.done.extend(new_done)

    # ---------- cache writer --------------------------------------- #
    async def _write_cache(self, reqs: List[APIRequestState]) -> None:
        if not ((self.params.cache or self.params.aggressive_cache) and reqs):
            return
        to_cache: Dict[str, Any] = {}
        for r in reqs:
            if r.response and r.response.valid:
                try:
                    model = self._resp_model.model_validate(r.response.response_data)
                    to_cache[r.key] = model
                except:
                    # validation failures are handled by BaseExecutor
                    pass 
        if to_cache:
            await self.redis.set_results(to_cache)

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
                logging.error("%s: POST failed - %s", self.log_id, exc)
                for req in batch:
                    req.response = StrategyResponse.failure("HTTP failure", str(exc))

        async def _bounded(batch: List[APIRequestState]) -> None:
            async with sem:
                await _send(batch)

        await asyncio.gather(*[_bounded(b) for b in batches])
        await self._write_cache(st.wave)
        st.done.extend(st.wave)
        st.wave = []
