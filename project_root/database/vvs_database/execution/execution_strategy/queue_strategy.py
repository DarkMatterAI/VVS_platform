import asyncio 
import pika
import json 
import time 
import random 

from typing import Dict, List, Tuple  

from vvs_database.schemas import ExecuteRequestUnion, ExecuteResponseUnion, ExecuteParams
from vvs_database.models import Plugin 
from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
from vvs_database.execution.connections import Connections #RedisService, RabbitMQService

class QueueExecutionStrategy(ExecutionStrategy):
    """Strategy for executing queue-based plugins"""

    def __init__(self, 
                 connections: Connections,
                 execute_params: ExecuteParams,
                 ):
        self.redis_service = connections.redis_service 
        self.rabbitmq_service = connections.rabbitmq_service
        self.execute_params = execute_params
        self.log_id = 'Queue Execute'
        self.connection = None 
        self.channel = None 
        
    async def execute(self, 
                plugin: Plugin, 
                requests: Dict[str, ExecuteRequestUnion]
                ) -> Dict[str, ExecuteResponseUnion]:
        """
        Execute plugin requests through RabbitMQ queue system with intelligent backoff
        
        Flow:
        1. Acquire semaphore locks for as many requests as possible
        2. Queue locked requests to RabbitMQ
        3. Calculate appropriate backoff time
        4. Poll Redis for results and release semaphores during backoff period
        5. Repeat until all requests are processed or timeout
        """
        print(f"{self.log_id}: Queueing {len(requests.keys())} requests via RabbitMQ")
        if not requests:
            return {}

        # Configuration
        timeout = plugin.timeout
        lock_timeout = int(1.1 * timeout)
        max_concurrency = plugin.max_concurrency
        semaphore_name = f"plugin:{plugin.id}"
        
        # Backoff settings
        base_backoff = max(0.5, min(2.0, timeout / max_concurrency))
        
        # Prepare request tracking structure
        request_tracker = self._prepare_requests(plugin, requests)
        
        # Track queue health
        queue_errors = 0
        max_queue_errors = 3
        
        # Main execution loop - continue until all requests processed or timed out
        pending_count = self._count_pending_requests(request_tracker)
        while pending_count > 0:
            print(f"{self.log_id}: Publishing messages: {pending_count} outstanding")
            
            # 1. Try to acquire semaphores for pending requests (just once per iteration)
            await self._acquire_available_locks(request_tracker, semaphore_name, max_concurrency, 
                                                lock_timeout, max_concurrency)
            
            # 2. Queue messages for newly acquired locks
            messages_to_queue = self._get_unqueued_messages(request_tracker)
            
            if messages_to_queue:
                try:
                    successful_ids = self.rabbitmq_service.publish_messages(messages_to_queue)
                    # successful_ids = self._publish_messages(messages_to_queue)
                    self._update_queued_status(request_tracker, successful_ids)
                    
                    # Handle queue errors
                    if len(successful_ids) < len(messages_to_queue):
                        queue_errors += 1
                        if queue_errors >= max_queue_errors:
                            self._mark_failed_messages(request_tracker)
                            
                except Exception as e:
                    print(f"{self.log_id}: Error in queue publishing: {str(e)}")
                    queue_errors += 1
                    if queue_errors >= max_queue_errors:
                        self._mark_failed_messages(request_tracker)
            
            # 3. Calculate appropriate backoff time
            pending_before = pending_count 
            pending_count = self._count_pending_requests(request_tracker)
            acquired_this_round = pending_before - pending_count
            print(f"{self.log_id}: Published {acquired_this_round} messages")

            backoff_time = max(self.execute_params.queue_polling_interval, base_backoff * (1 + random.random() * 0.2))
            
            # 4. Poll for results and handle timeouts during backoff period
            backoff_start = time.time()
            print(f"{self.log_id}: Backoff time {backoff_time}")

            remaining_backoff = float('inf') # init to inf to guarantee one poll
            while remaining_backoff > self.execute_params.queue_polling_interval and pending_count > 0:
                poll_interval = min(remaining_backoff, self.execute_params.queue_polling_interval)
                await self._poll_and_release_cycle(request_tracker, plugin, semaphore_name, poll_interval)
                remaining_backoff = backoff_time - (time.time() - backoff_start)
                pending_count = self._count_pending_requests(request_tracker)

            print(f"{self.log_id}: {pending_count} requests still pending/processing/queued")
        
        # Compile final results
        return self._compile_results(request_tracker)
    
    async def _poll_and_release_cycle(self, request_tracker, plugin, semaphore_name, wait_time):
        """
        Run a single poll-and-release cycle with a wait time
        
        Args:
            request_tracker: The request tracking structure
            plugin: The plugin being executed
            semaphore_name: Name of the semaphore for this plugin
            wait_time: Time to wait after polling before returning
        """
        # Poll for results
        identifiers_to_release = await self._poll_for_results(request_tracker)
        
        # Release semaphores for completed requests
        if identifiers_to_release and self.execute_params.use_semaphore:
            await self.redis_service.release_semaphore(semaphore_name, identifiers_to_release)
        
        # Check for timeouts
        timeout_identifiers = await self._check_for_timeouts(request_tracker, plugin.timeout)
        
        # Release semaphores for timed-out requests
        if timeout_identifiers and self.execute_params.use_semaphore:
            await self.redis_service.release_semaphore(semaphore_name, timeout_identifiers)
        
        # Wait before next cycle if needed
        if wait_time > 0:
            await asyncio.sleep(wait_time)
    
    def _prepare_requests(self, plugin, requests):
        """
        Prepare the request tracking structure
        
        Args:
            plugin: The plugin being executed
            requests: Dictionary of request objects
            
        Returns:
            Dictionary tracking the state of each request
        """
        request_tracker = {}
        for key, request in requests.items():
            request_obj = self.populate_request_id(plugin, request)
            request_id = request_obj.request_data.request_id
            request_tracker[key] = {
                "request": request_obj,
                "request_id": request_id,
                "response_id": request_id.replace('request', 'response').replace('.', ':'),
                "status": "pending",  # pending, processing, queued, completed, error
                "queued_at": None,
                "identifier": None,  # semaphore identifier
                "result": None,
                "semaphore_count": 0
            }
        return request_tracker
    
    def _count_pending_requests(self, request_tracker):
        """Count requests that are still in progress"""
        return sum(1 for req_data in request_tracker.values() 
                  if req_data["status"] in ["pending", "processing", "queued"])
    
    def _get_unqueued_messages(self, request_tracker):
        """Get messages that have lock but haven't been queued yet"""
        return [
            req_data["request"] 
            for req_data in request_tracker.values()
            if req_data["status"] == "processing" and req_data["queued_at"] is None
        ]
    
    def _update_queued_status(self, request_tracker, successful_ids):
        """Update status for successfully queued messages"""
        current_time = time.time()
        for req_data in request_tracker.values():
            if req_data["status"] == "processing" and req_data["request_id"] in successful_ids:
                req_data["queued_at"] = current_time
                req_data["status"] = "queued"
    
    def _mark_failed_messages(self, request_tracker):
        """Mark messages that failed to queue as error"""
        print(f"{self.log_id}: Too many queue errors, marking failed messages as error")
        for req_data in request_tracker.values():
            if req_data["status"] == "processing" and req_data["queued_at"] is None:
                req_data["status"] = "error"
                req_data["result"] = {"valid": False, 
                                      "response_data": None, 
                                      "failure_reason": "Queue error",
                                      "failure_detail": "Too many queue errors, marking failed messages as error"
                                    }
    
    async def _acquire_available_locks(self, 
                                       request_tracker, 
                                       semaphore_name, 
                                       max_locks, 
                                       lock_timeout, 
                                       max_concurrency):
        """
        Try to acquire locks for pending requests (just one try per request per iteration)
        
        Args:
            request_tracker: The request tracking structure
            semaphore_name: Name of the semaphore to acquire
            max_locks: Maximum number of concurrent locks
            lock_timeout: How long until the lock expires
        """
        if not self.execute_params.use_semaphore:
            for key, req_data in request_tracker.items():
                if req_data["status"] == "pending":
                    req_data["status"] = "processing"
                    req_data["identifier"] = None
            return # early exit without semaphore 

        print(f"{self.log_id}: Acquiring locks")
        lock_count = 0
        for key, req_data in request_tracker.items():
            if req_data["status"] == "pending":
                success, identifier = await self.redis_service.acquire_semaphore(
                    name=semaphore_name,
                    max_locks=max_locks,
                    lock_timeout=lock_timeout,
                    max_attempts=1,  # Just try once per iteration
                    initial_backoff=0.1,  # These values don't matter for max_attempts=1
                    max_backoff=1.0,
                    backoff_factor=1.0
                )
                req_data["semaphore_count"] += 1
                
                if success:
                    req_data["status"] = "processing"
                    req_data["identifier"] = identifier
                    lock_count += 1
                else:
                    break 

                if lock_count >= max_concurrency:
                    # early exit if we acquire all locks 
                    break 

        print(f"{self.log_id}: Acquired {lock_count} locks")
    
    async def _poll_for_results(self, request_tracker):
        """
        Poll Redis for results in a batch operation
        
        Args:
            request_tracker: The request tracking structure
            
        Returns:
            List of semaphore identifiers to release
        """
        print(f"{self.log_id}: Polling results")

        # Collect request IDs to poll
        processing_requests = {
            req_data["response_id"]: (key, req_data) 
            for key, req_data in request_tracker.items()
            if req_data["status"] == "queued" and req_data["queued_at"] is not None
        }
        
        identifiers_to_release = []
        
        if not processing_requests:
            return identifiers_to_release
        
        # Get results in a single Redis operation
        request_ids = list(processing_requests.keys())
        results = await self.redis_service.get_results(request_ids, delete=True)
        
        # Process results
        for response_id, result in results.items():
            if response_id in processing_requests:
                orig_key, req_data = processing_requests[response_id]
                req_data["status"] = "completed"
                req_data["result"] = result 
                
                # Add this identifier to the release list
                if req_data["identifier"]:
                    identifiers_to_release.append(req_data["identifier"])
                    req_data["identifier"] = None  # Mark as scheduled for release
        
        print(f"{self.log_id}: Found {len(identifiers_to_release)} results")
        return identifiers_to_release
    
    async def _check_for_timeouts(self, request_tracker, timeout):
        """
        Check for timed out requests
        
        Args:
            request_tracker: The request tracking structure
            timeout: Timeout duration in seconds
            
        Returns:
            List of semaphore identifiers to release
        """
        print(f"{self.log_id}: Checking timeouts")
        timeout_count = 0
        semaphore_count = 0
        current_time = time.time()
        identifiers_to_release = []

        for key, req_data in request_tracker.items():
            fail_request = False 
            failure_reason = None 
            failure_detail = None 

            # timeout on queue
            if req_data["status"] == "queued" and req_data["queued_at"] is not None:
                elapsed = current_time - req_data["queued_at"]
                if elapsed > timeout:
                    fail_request = True 
                    timeout_count += 1
                    failure_reason = "Queue timeout error"
                    failure_detail = "Failed to return message response in time "

            # unable to acquire semaphore 
            if ((req_data["status"] not in ["complete", "error"]) and 
                (req_data["semaphore_count"] > self.execute_params.max_semaphore_attempts)):
                fail_request = True 
                semaphore_count += 1 
                failure_reason = "Semaphore failure"
                failure_detail = f"Unable to acquire semaphore after {self.execute_params.max_semaphore_attempts} attempts"

            if fail_request:
                req_data["status"] = "error"
                req_data["result"] = {"valid": False, 
                        "response_data": None, 
                        "failure_reason": failure_reason,
                        "failure_detail": failure_detail
                    }
                if req_data["identifier"]:
                    identifiers_to_release.append(req_data["identifier"])
                    req_data["identifier"] = None  # Mark as scheduled for release

        print(f"{self.log_id}: Found {timeout_count} message timeouts, {semaphore_count} semaphore timeouts")
        
        return identifiers_to_release
    
    def _compile_results(self, request_tracker):
        """
        Compile final results from request tracker
        
        Args:
            request_tracker: The request tracking structure
            
        Returns:
            Dictionary mapping original keys to results
        """
        results = {}
        for key, req_data in request_tracker.items():
            results[key] = req_data['result']
        return results

