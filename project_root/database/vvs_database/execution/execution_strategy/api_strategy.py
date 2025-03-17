import asyncio 

from typing import Dict, List, Tuple  

from vvs_database.schemas import ExecuteRequestUnion, ExecuteResponseUnion, ExecuteParams
from vvs_database.models import Plugin 
from vvs_database.utils import make_post_request
from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
from vvs_database.execution.connections import Connections #RedisService

async def concurrency_bounded_func(semaphore, func, input, kwargs):
    """Run function within concurrency limit."""
    async with semaphore:
        output = await func(input, **kwargs)
    return output

async def concurrency_wrapper(concurrency, func, iterable, kwargs):
    """Control in-process concurrency"""
    semaphore = asyncio.Semaphore(concurrency)
    
    tasks = [concurrency_bounded_func(semaphore, func, item, kwargs) for item in iterable]
    results = await asyncio.gather(*tasks)
    return results


class APIExecutionStrategy(ExecutionStrategy):
    """Strategy for executing API-based plugins"""
    def __init__(self, 
                 connections: Connections,
                 execute_params: ExecuteParams,
                 ):
        self.redis_service = connections.redis_service 
        self.execute_params = execute_params
        self.log_id = 'API Execute'

    def batch_requests(self, 
                       request_list: List[Tuple[str, ExecuteRequestUnion]],
                       batch_size: int
                       ):
        if batch_size == 1:
            return request_list 
        
        batches = [request_list[i:i+batch_size] 
                   for i in range(0, len(request_list), batch_size)]
        return batches 
    
    def _add_failure_result(self, batch, failure_reason, failure_detail):
        failure_result = {"valid": False, 
                          "response_data": None, 
                          "failure_reason": failure_reason, 
                          "failure_detail": failure_detail}
        for request in batch:
            request["response"] = failure_result
        

    async def execute(self, 
                      plugin: Plugin, 
                      requests: Dict[str, ExecuteRequestUnion]
                      ) -> Dict[str, ExecuteResponseUnion]:
        print(f"{self.log_id}: Executing {len(requests.keys())} requests")
        if not requests:
            return {}

        url = plugin.endpoint_url
        timeout = plugin.timeout
        lock_timeout = int(1.1*timeout) # lock timeout longer than request timeout
        retries = plugin.max_retries
        batch_size = plugin.batch_size
        max_concurrency = plugin.max_concurrency
        initial_backoff = timeout/max_concurrency # guess initial lock backoff
        semaphore_name = f"plugin:{plugin.id}"
        log_id = self.log_id 

        request_list = [{"key": key,
                         "request": request} #self.populate_request_id(plugin, request)}
                         for key,request in requests.items()]
        request_batches = self.batch_requests(request_list, batch_size)

        async def process_batch(batch):
            is_single_item = not isinstance(batch, list)
            if is_single_item:
                batch = [batch]

            # Try to acquire semaphore with built-in retry/backoff
            if self.execute_params.use_semaphore:
                success, identifier = await self.redis_service.acquire_semaphore(
                    name=semaphore_name, 
                    max_locks=max_concurrency,
                    lock_timeout=lock_timeout,
                    max_attempts=self.execute_params.max_semaphore_attempts,
                    initial_backoff=initial_backoff,
                    max_backoff=timeout,
                    backoff_factor=self.execute_params.backoff_factor
                )
            else:
                success = True 
                identifier = None 

            try:
                if not success:
                    print(f"{self.log_id}: Failed to acquire semaphore")
                    detail = f"Unable to acquire semaphore after {self.execute_params.max_semaphore_attempts} attempts"
                    self._add_failure_result(batch, "Semaphore failure", detail)
                else:
                    request = [i['request'].model_dump() for i in batch]
                    if is_single_item:
                        response = await make_post_request(request[0], url, timeout, retries, retry_sleep=1.0, log_id=log_id)
                        response = [response]
                    else:
                        response = await make_post_request(request, url, timeout, retries, retry_sleep=1.0, log_id=log_id)

                    for i, request in enumerate(batch):
                        request["response"] = {"valid": True, "response_data": response[i]}

                batch = batch[0] if is_single_item else batch 
                return batch 

            except Exception as e:
                print(f"{self.log_id}: Post request to {url} failed - {str(e)}")
                self._add_failure_result(batch, "Post request failure", f"{str(e)}")
                batch = batch[0] if is_single_item else batch 
                return batch 
            finally:
                if self.execute_params.use_semaphore and (identifier is not None):
                    await self.redis_service.release_semaphore(semaphore_name, [identifier])

        batch_results = await concurrency_wrapper(max_concurrency, process_batch, request_batches, {})
        results = []
        for batch_result in batch_results:
            if isinstance(batch_result, list):
                results.extend(batch_result)
            else:
                results.append(batch_result)

        results = {i['key']:i['response'] for i in results}
        return results 

