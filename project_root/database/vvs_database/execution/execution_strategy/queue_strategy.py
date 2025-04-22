import asyncio
import time
import random
from typing import Dict, List, Tuple

from vvs_database.schemas import (
    ExecuteRequestUnion,
    ExecuteResponseUnion,
    ExecuteParams,
    PluginInDB,
)
from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
from vvs_database.execution.connections import Connections
from vvs_database import logging


class QueueExecutionStrategy(ExecutionStrategy):
    """
    Execute requests via RabbitMQ and poll Redis for responses.
    Each *batch* of messages is published once a semaphore token has been
    obtained; tokens are acquired in bulk to minimise Redis chatter.
    """

    # --------------------------- life-cycle ---------------------------------

    def __init__(self, connections: Connections, execute_params: ExecuteParams):
        self.redis_service = connections.redis_service
        self.rabbitmq_service = connections.rabbitmq_service
        self.execute_params = execute_params
        self.log_id = "QueueExecute"

    # --------------------------- public API ---------------------------------

    async def execute(
        self,
        plugin: PluginInDB,
        requests: Dict[str, ExecuteRequestUnion],
    ) -> Dict[str, ExecuteResponseUnion]:
        logging.info(f"{self.log_id}: Queueing {len(requests.keys())} requests via RabbitMQ")
        if not requests:
            return {}

        # ── plugin-level knobs ─────────────────────────────────────────────
        timeout          = plugin.timeout                     # sec
        lock_timeout     = int(timeout * 1.1)
        max_concurrency  = plugin.max_concurrency
        batch_size       = plugin.batch_size
        semaphore_name   = f"plugin:{plugin.id}"

        # ── build RequestTracker dict ─────────────────────────────────────
        tracker: Dict[str, dict] = {}
        for k, req in requests.items():
            rid = req.request_data.request_id
            tracker[k] = {
                "request":        req,
                "req_id":         rid,
                "resp_id":        rid.replace("request", "response").replace(".", ":"),
                "status":         "waiting",     # waiting | queued | done | error
                "queued_at":      None,
                "identifier":     None,          # semaphore token
                "attempts_left":  self.execute_params.max_semaphore_attempts,
                "queue_errors":   0,
                "result":         None,
            }

        # split requests into *batches* that will share one token
        pending_batches: List[List[str]] = []   # list[ list[key] ]
        objs = list(tracker.keys())
        for i in range(0, len(objs), batch_size):
            pending_batches.append(objs[i : i + batch_size])

        # ── constants for back-off & timeout checks ───────────────────────
        poll_interval   = self.execute_params.queue_polling_interval
        base_backoff    = max(0.5, min(2.0, timeout / max_concurrency))
        backoff_factor  = self.execute_params.backoff_factor
        max_queue_err   = 3

        # ── main loop: continue while ANY request unfinished ──────────────
        backoff_current = base_backoff

        while True:
            unfinished = [
                k for k, v in tracker.items()
                if v["status"] in ("waiting", "processing", "queued")
            ]
            if not unfinished:
                break   # all done / error ➜ exit

            # ----------------------------------------------------------
            # Build (or rebuild) batches that still need publishing
            # ----------------------------------------------------------
            pending_batches = self._rebuild_waiting_batches(tracker, batch_size)
            poll_count = len(unfinished) - len(pending_batches)
            logging.info(f"{self.log_id}: Queue loop - {len(pending_batches)} unpublished, {poll_count} outstanding")

            # ───── No more to publish → just poll/timeout & sleep ─────
            if not pending_batches:
                await self._poll_results(plugin, tracker)
                self._apply_timeouts_and_errors(tracker, timeout, max_queue_err)
                await asyncio.sleep(poll_interval)
                continue

            # ----------------------------------------------------------
            # 1) Acquire semaphore tokens (or dummy tokens)
            # ----------------------------------------------------------
            n_need  = min(len(pending_batches), max_concurrency)
            if self.execute_params.use_semaphore:
                tokens = await self.redis_service.acquire_semaphores_batch(
                    name=semaphore_name,
                    n=n_need,
                    max_locks=max_concurrency,
                    lock_timeout=lock_timeout,
                )
            else:
                tokens = [None] * n_need        # semaphore disabled

            if not tokens:                      # no tokens this round
                await self._handle_no_tokens(pending_batches, tracker)
                jitter = 0.8 + random.random() * 0.4
                await asyncio.sleep(backoff_current * jitter)
                backoff_current = min(backoff_current * backoff_factor, timeout)
                self._apply_timeouts_and_errors(tracker, timeout, max_queue_err)
                continue

            backoff_current = base_backoff      # progress → reset back‑off

            # ----------------------------------------------------------
            # 2) Launch wave for len(tokens) batches
            # ----------------------------------------------------------
            wave_batches = [pending_batches.pop(0) for _ in range(len(tokens))]

            # mark “processing” + remember token
            for batch_keys, tok in zip(wave_batches, tokens):
                for k in batch_keys:
                    tracker[k]["status"] = "processing"
                    tracker[k]["identifier"] = tok

            await asyncio.gather(
                *[self._publish_batch(plugin, b, tracker) for b in wave_batches]
            )

            # release real tokens
            real_tokens = [t for t in tokens if t]
            if self.execute_params.use_semaphore and real_tokens:
                await self.redis_service.release_semaphore(semaphore_name, real_tokens)

            # ----------------------------------------------------------
            # 3) After publishing, poll / handle errors immediately
            # ----------------------------------------------------------
            await self._poll_results(plugin, tracker)
            self._apply_timeouts_and_errors(tracker, timeout, max_queue_err)

        # ── compile final dict[orig_key] → ExecuteResponseUnion ────────────
        return {k: v["result"] for k, v in tracker.items()}

    # --------------------------- helpers -----------------------------------

    async def _publish_batch(self, plugin, batch_keys: List[str], tracker: Dict[str, dict]):
        """Try to publish a batch; on failure increment queue_errors per key."""
        messages = [tracker[k]["request"] for k in batch_keys]
        try:
            sent_ids = self.rabbitmq_service.publish_messages(messages)
        except Exception as exc:  # network / channel failure
            logging.error("%s: RabbitMQ publish failed - %s", self.log_id, exc)
            sent_ids = []

        now = time.time()
        sent_set = set(sent_ids)
        for k in batch_keys:
            if tracker[k]["req_id"] in sent_set:
                tracker[k]["status"] = "queued"
                tracker[k]["queued_at"] = now
            else:  # failed publish
                tracker[k]["queue_errors"] += 1

    async def _poll_results(self, plugin, tracker: Dict[str, dict]):
        """Fetch any available responses from Redis in one MGET."""
        polling_ids = [
            v["resp_id"] for v in tracker.values() if v["status"] == "queued"
        ]
        if not polling_ids:
            return 
        res = await self.redis_service.get_results(polling_ids, delete=True)
        for resp_id, payload in res.items():
            for k, meta in tracker.items():
                if meta["resp_id"] == resp_id:
                    meta["status"] = "done"
                    meta["result"] = payload
                    break

    def _apply_timeouts_and_errors(self, tracker, timeout, max_queue_err):
        """Mark requests error if queue timeout or too many publish failures."""
        now = time.time()
        for meta in tracker.values():
            if meta["status"] == "done" or meta["status"] == "error":
                continue

            # semaphore exhaustion
            if meta["attempts_left"] <= 0:
                logging.error(f"{self.log_id}: Message failed to acquire semaphore")
                self._fail(meta, "Semaphore failure",
                           f"Exceeded max attempts ({self.execute_params.max_semaphore_attempts})")
                continue

            # publish failures
            if meta["queue_errors"] > max_queue_err:
                logging.error(f"{self.log_id}: Message failed to post to queue")
                self._fail(meta, "Queue error", "Exceeded publish retries")
                continue

            # queue timeout
            if meta["status"] == "queued" and meta["queued_at"] is not None:
                if now - meta["queued_at"] > timeout:
                    logging.error(f"{self.log_id}: Message failed timeout")
                    self._fail(meta, "Queue timeout", "No response within timeout")

    def _fail(self, meta, reason, detail):
        meta["status"] = "error"
        meta["result"] = {
            "valid": False,
            "response_data": None,
            "failure_reason": reason,
            "failure_detail": detail,
        }

    async def _handle_no_tokens(self, pending_batches, tracker):
        """Decrement attempts for batches that *need* a token."""
        for batch_keys in pending_batches:
            for k in batch_keys:
                tracker[k]["attempts_left"] -= 1

    def _rebuild_waiting_batches(self, tracker, batch_size):
        """
        Re-chunk not-yet-sent requests (status waiting|processing) into
        fresh batches.  'queued' items stay out so we never republish them.
        """
        waiting_keys = [
            k for k, v in tracker.items()
            if v["status"] in ("waiting", "processing")
        ]
        return [
            waiting_keys[i : i + batch_size]
            for i in range(0, len(waiting_keys), batch_size)
        ]
