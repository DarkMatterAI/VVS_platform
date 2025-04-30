from __future__ import annotations

import asyncio, json, uuid
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

from vvs_database import logging
from vvs_database.execution.connections import Connections
from vvs_database.execution.execution_strategy import (
    APIExecutionStrategy,
    QueueExecutionStrategy,
)
from vvs_database.schemas import (
    ExecuteParams,
    ExecuteRequestUnion,
    ExecuteResponseUnion,
    ExecutionSources,
    PluginInDB,
)
from vvs_database.schemas.internal_schemas import ExecutionLog


@dataclass
class _CacheCheckResult:
    cached:     Dict[str, ExecuteResponseUnion]
    db:         Dict[str, ExecuteResponseUnion]
    remaining:  Dict[str, ExecuteRequestUnion]


class BasePluginExecutor:
    """
    Common scaffolding for all concrete plugin executors.

    Sub-classes must set:

        request_model   - Pydantic model of the request
        response_model  - Pydantic model of the response
        update_params() - tweak ExecuteParams if needed
        query_database() / check_in_results() - plugin-specific DB ops
    """

    request_model:  Type  = None   # to be provided by sub-class
    response_model: Type  = None   # to be provided by sub-class

    # ───────────────────────────────────────────────────────────────── #
    # life-cycle                                                      #
    # ───────────────────────────────────────────────────────────────── #
    def __init__(
        self,
        plugin: PluginInDB,
        connections: Connections,
        execute_params: ExecuteParams,
    ) -> None:
        self._init_strategy(plugin, connections, execute_params)

    # ---------------------------------------------------------------- #
    def _init_strategy(
        self,
        plugin: PluginInDB,
        connections: Connections,
        execute_params: ExecuteParams,
    ) -> None:
        """Decide which execution strategy to use + prime bookkeeping."""
        self.plugin       = plugin
        self.connections  = connections
        self.execute_params = self.update_params(execute_params)

        self.execution_strategy = (
            APIExecutionStrategy(connections, self.execute_params, self.response_model)
            if plugin.execution_type == "api"
            else QueueExecutionStrategy(connections, self.execute_params, self.response_model)
        )

        # log chain
        self.log_id        = ""
        self.execution_log: Optional[ExecutionLog] = None
        self.execution_logs: List[ExecutionLog]    = []

    # ───────────────────────────────────────────────────────────────── #
    # overridable hooks                                                #
    # ───────────────────────────────────────────────────────────────── #
    def update_params(self, params: ExecuteParams) -> ExecuteParams:  # noqa: D401
        """Sub-classes may mutate the ExecuteParams before use."""
        return params

    async def query_database(
        self,
        plugin:   PluginInDB,
        requests: Dict[str, ExecuteRequestUnion],
    ) -> Dict[str, ExecuteResponseUnion]:
        return {}   # overridden by sub-classes

    async def check_in_results(
        self,
        requests:        List[ExecuteRequestUnion],
        results:         List[ExecuteResponseUnion],
        valid_execution: List[bool],
    ) -> Any:
        return None  # overridden by sub-classes

    # ───────────────────────────────────────────────────────────────── #
    # public helpers                                                  #
    # ───────────────────────────────────────────────────────────────── #
    def init_log_id(self, root_id: Optional[str] = None) -> None:
        if root_id is None:
            root_id = str(uuid.uuid4())

        self.log_id = f"{root_id}:Executor"
        self.connections.init_log_id(root_id)
        self.execution_strategy.log_id = f"{root_id}:Execute {self.plugin.execution_type}"

    def init_execution_log(self) -> None:
        if self.execution_log is not None:
            self.execution_logs.append(self.execution_log)
        self.execution_log = ExecutionLog.from_plugin_record(
            self.plugin, self.execute_params
        )

    async def close(self) -> None:
        await self.connections.close()

    # ───────────────────────────────────────────────────────────────── #
    # main high-level entry point                                     #
    # ───────────────────────────────────────────────────────────────── #
    async def execute(
        self,
        requests: List[ExecuteRequestUnion],
        log_id: Optional[str] = None,
    ) -> Tuple[List[ExecuteResponseUnion], Optional[Any], List[bool]]:

        self.init_log_id(log_id)
        self.init_execution_log()

        if not requests:
            logging.info("%s: No requests - returning", self.log_id)
            return [], None, []

        logging.info(
            "%s: Executing %d requests for plugin %d (%s)",
            self.log_id,
            len(requests),
            self.plugin.id,
            self.plugin.type,
        )

        # 1. validate + annotate requests
        requests_validated = self._validate_and_tag(requests)

        # 2. deduplicate → unique key → original index map
        key_to_req, key_to_idx = self._deduplicate(requests_validated)

        # 3. cache / DB lookup
        check_res = await self._check_cache_and_db(key_to_req)

        # 4. execute remaining (uncached, not in DB)
        executed = await self._execute_remaining(check_res.remaining)

        # 5. merge all sources + reassemble to original order
        merged = {**check_res.cached, **check_res.db, **executed}
        results, valid_mask = self._reassemble(
            requests_validated, merged, key_to_idx
        )

        # 6. optional check-in to DB
        checkin = await self.check_in_results(requests_validated, results, valid_mask)

        return results, checkin, valid_mask

    # ───────────────────────────────────────────────────────────────── #
    # pipeline steps (private)                                         #
    # ───────────────────────────────────────────────────────────────── #
    def _validate_and_tag(
        self,
        requests: List[ExecuteRequestUnion],
    ) -> List[ExecuteRequestUnion]:
        """pydantic-validate + inject `.request_data`."""
        out: List[ExecuteRequestUnion] = []
        for req in requests:
            req = self.request_model.model_validate(req)
            req = self.plugin.populate_request_data(req)
            out.append(req)
        return out

    # ---------------------------------------------------------------- #
    def _deduplicate(
        self,
        requests: List[ExecuteRequestUnion],
    ) -> Tuple[
        Dict[str, ExecuteRequestUnion],
        Dict[str, List[int]],
    ]:
        keys = [r.generate_key(plugin_id=self.plugin.id) for r in requests]
        self.execution_log.record_inputs(keys, self.request_model.strip_key)

        key_to_req: Dict[str, ExecuteRequestUnion] = {}
        key_to_idx: Dict[str, List[int]]           = {}
        for idx, (req, key) in enumerate(zip(requests, keys)):
            key_to_req.setdefault(key, req)
            key_to_idx.setdefault(key, []).append(idx)

        self.execution_log.record_unique_inputs(len(key_to_req))
        return key_to_req, key_to_idx

    # ---------------------------------------------------------------- #
    async def _check_cache_and_db(
        self,
        key_to_req: Dict[str, ExecuteRequestUnion],
    ) -> _CacheCheckResult:

        if not (self.execute_params.cache or self.execute_params.db_lookup):
            return _CacheCheckResult({}, {}, key_to_req)

        keys = list(key_to_req)
        logging.info("%s: Cache/DB check for %d keys", self.log_id, len(keys))

        # ---- cache ---------------------------------------------------
        cached_raw = (
            await self.connections.redis_service.get_results(keys)
            if self.execute_params.cache and keys
            else {}
        )
        cached = {
            k: self.response_model.model_validate(v) for k, v in cached_raw.items()
        }
        self.execution_log.record_cache_hits(cached, self.request_model.strip_key)

        # ---- DB ------------------------------------------------------
        uncached = {k: key_to_req[k] for k in keys if k not in cached}
        db = await self.query_database(self.plugin, uncached)
        self.execution_log.record_db_hits(db, self.request_model.strip_key)

        # ---- remainder ----------------------------------------------
        remaining = {k: uncached[k] for k in uncached if k not in db}
        return _CacheCheckResult(cached, db, remaining)

    # ---------------------------------------------------------------- #
    async def _execute_remaining(
        self,
        remaining: Dict[str, ExecuteRequestUnion],
    ) -> Dict[str, ExecuteResponseUnion]:

        if not remaining:
            return {}

        logging.info("%s: Executing %d uncached requests", self.log_id, len(remaining))

        raw = await self.execution_strategy.execute(self.plugin, remaining)

        executed: Dict[str, ExecuteResponseUnion] = {}
        failed:   List[Tuple[ExecuteRequestUnion, Dict[str, Any]]] = []
        sources   = {k: [] for k in ExecutionSources}

        for key, payload in raw.items():
            if payload.valid:
                try:
                    data = self.response_model.model_validate(payload.response_data)
                    executed[key] = data
                    sources[payload.source].append(key)
                except Exception as exc:
                    payload.valid = False
                    payload.failure_reason  = "Model validation error"
                    payload.failure_detail  = f"{exc}; response={payload.response_data}"

            if not payload.valid:
                logging.error("%s: Exec failure - %s", self.log_id, payload.failure_reason)
                # failed.append((remaining[key], payload.model_dump()))
                failed.append((remaining[key], payload))
                sources[ExecutionSources.FAILURE].append(key)

        await self.connections.db_service.log_failed_requests(self.plugin, failed)

        # if self.execute_params.cache and executed:
            # await self.connections.redis_service.set_results(executed)

        self.execution_log.record_executed(sources, self.request_model.strip_key)
        return executed

    # ---------------------------------------------------------------- #
    def _reassemble(
        self,
        original: List[ExecuteRequestUnion],
        merged:   Dict[str, ExecuteResponseUnion],
        key_to_idx: Dict[str, List[int]],
    ) -> Tuple[List[ExecuteResponseUnion], List[bool]]:

        out:   List[Optional[ExecuteResponseUnion]] = [None] * len(original)
        flags: List[bool]                           = [True] * len(original)

        for key, idxs in key_to_idx.items():
            if key not in merged:
                continue
            for i in idxs:
                out[i] = merged[key]

        # fill gaps with failure responses
        fail_resp = self.response_model.failure_response()
        for i, res in enumerate(out):
            if res is None:
                out[i] = fail_resp
                flags[i] = False

        # type checker friendly cast
        return out, flags

