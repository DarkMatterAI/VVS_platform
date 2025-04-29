"""
Typed helper objects used by QueueExecutionStrategy and APIExecutionStrategy.
They only live inside the strategies; the public surface that other
modules consume (dicts with 'valid', 'response_data', …) is unchanged.
"""

from __future__ import annotations
from typing import Optional, Any, Literal
from pydantic import BaseModel, Field, PrivateAttr
import random 

from vvs_database.schemas import (
    ExecuteRequestUnion,
    ExecutionSources,
)

class StrategyResponse(BaseModel):
    """
    Normalised execution-result used INTERNALLY by the strategies.
    """
    valid: bool
    response_data: Optional[Any]
    source: ExecutionSources
    failure_reason: Optional[str] = None
    failure_detail: Optional[str] = None

    @classmethod
    def failure(cls, reason: str, detail: str) -> "StrategyResponse":
        return cls(
            valid=False,
            response_data=None,
            source=ExecutionSources.FAILURE,
            failure_reason=reason,
            failure_detail=detail,
        )

    def as_legacy_dict(self) -> dict:
        """
        Transform into the dict shape expected by BasePluginExecutor.
        """
        return self.model_dump()


class APIRequestState(BaseModel):
    key: str
    request: ExecuteRequestUnion
    attempts_left: int
    response: Optional[StrategyResponse] = None


class QueueRequestState(BaseModel):
    key: str
    request: ExecuteRequestUnion
    req_id: str
    resp_id: str
    status: Literal["waiting", "processing", "queued", "done", "error"] = "waiting"
    queued_at: Optional[float] = None
    identifier: Optional[str] = None          # semaphore token
    attempts_left: int
    queue_errors: int = 0
    response: Optional[StrategyResponse] = None


class _BaseConst(BaseModel):
    timeout:     float
    lock_t:      int
    bs:          int
    max_conc:    int
    back_factor: float

    # private attrs (not part of the public model schema)
    _back_base: float = PrivateAttr()
    _back_cur:  float = PrivateAttr()

    # ------------------------------------------------------------------ #
    #  back-off helpers
    # ------------------------------------------------------------------ #
    def initialise_backoff(self, base: float) -> None:
        self._back_base = base
        self._back_cur  = base

    def backoff_sleep(self) -> float:
        """Compute *and update* the next back-off delay (seconds)."""
        jitter = 0.8 + random.random() * 0.4
        sleep  = self._back_cur * jitter
        self._back_cur = min(self._back_cur * self.back_factor, self.timeout)
        return sleep

    def reset_backoff(self) -> None:
        self._back_cur = self._back_base


class APIConst(_BaseConst):
    url:      str
    retries:  int
    sem_name: str

    @classmethod
    def from_plugin(cls, plugin, params) -> "APIConst":
        obj = cls(
            url         = plugin.endpoint_url,
            timeout     = plugin.timeout,
            lock_t      = int(plugin.timeout * 1.1),
            retries     = plugin.max_retries,
            bs          = plugin.batch_size,
            max_conc    = plugin.max_concurrency,
            sem_name    = f"plugin:{plugin.id}",
            back_factor = params.backoff_factor,
        )
        obj.initialise_backoff(plugin.timeout / plugin.max_concurrency)
        return obj


class QueueConst(_BaseConst):
    poll_interval: float
    sem_name:      str
    use_sema:      bool
    max_attempts:  int

    @classmethod
    def from_plugin(cls, plugin, params) -> "QueueConst":
        obj = cls(
            timeout       = plugin.timeout,
            lock_t        = int(plugin.timeout * 1.1),
            bs            = plugin.batch_size,
            max_conc      = plugin.max_concurrency,
            sem_name      = f"plugin:{plugin.id}",
            poll_interval = params.queue_polling_interval,
            use_sema      = params.use_semaphore,
            max_attempts  = params.max_semaphore_attempts,
            back_factor   = params.backoff_factor,
        )
        base = max(0.5, min(2.0, plugin.timeout / plugin.max_concurrency))
        obj.initialise_backoff(base)
        return obj
