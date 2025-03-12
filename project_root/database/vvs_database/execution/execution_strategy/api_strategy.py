import httpx 
import asyncio 

from typing import Dict, List, Tuple  

from vvs_database.schemas import ExecuteRequestUnion, ExecuteResponseUnion
from vvs_database.models import Plugin 
from vvs_database.exceptions import SemaphoreException
from vvs_database.execution.execution_strategy.base_strategy import ExecutionStrategy
from vvs_database.execution.redis import RedisService

async def make_post_request(data, url, timeout, retries, retry_sleep=0, log_id=''):
    """Make HTTP POST request with retry logic."""
    async with httpx.AsyncClient() as client:
        for attempt in range(retries + 1):
            print(f"{log_id}: Post Request to {url} attempt {attempt+1}")
            try:
                response = await client.post(url, json=data, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                # Handle HTTP status errors (400-599)
                status_code = e.response.status_code
                error_message = f"HTTP {status_code}"
                
                if status_code == 400:
                    error_message += " Bad Request: The server rejected the request as malformed"
                elif status_code == 401:
                    error_message += " Unauthorized: Authentication is required"
                elif status_code == 403:
                    error_message += " Forbidden: You don't have permission to access this resource"
                elif status_code == 404:
                    error_message += " Not Found: The requested resource does not exist"
                elif status_code == 429:
                    error_message += " Too Many Requests: Rate limit exceeded"
                elif 500 <= status_code < 600:
                    error_message += " Server Error: The server failed to fulfill the request"
                
                error_message += f"\nURL: {url}\nResponse: {e.response.text}"
                
                if attempt == retries:
                    raise httpx.HTTPStatusError(error_message, request=e.request, response=e.response)
            except httpx.TimeoutException as e:
                if attempt == retries:
                    raise httpx.TimeoutException(f"Request to {url} timed out after {timeout} seconds")
            except httpx.ConnectError as e:
                if attempt == retries:
                    raise httpx.ConnectError(f"Failed to connect to {url}: {str(e)}")
            except httpx.RequestError as e:
                if attempt == retries:
                    raise httpx.RequestError(f"Request to {url} failed: {str(e)}")
                
            if retry_sleep > 0:
                print(f"{log_id}: Post request failed, sleeping for {retry_sleep} seconds")
                await asyncio.sleep(retry_sleep) 

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
                 redis_service: RedisService,
                 use_semaphore: bool=True,
                 max_semaphore_attempts: int=20
                 ):
        self.redis_service = redis_service 
        self.log_id = 'API Execute'
        self.use_semaphore = use_semaphore
        self.max_semaphore_attempts = max_semaphore_attempts

    def batch_requests(self, 
                       request_list: List[Tuple[str, ExecuteRequestUnion]],
                       batch_size: int
                       ):
        if batch_size == 1:
            return request_list 
        
        batches = [request_list[i:i+batch_size] 
                   for i in range(0, len(request_list), batch_size)]
        return batches 
        

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
                         "request": self.populate_request_id(plugin, request)}
                         for key,request in requests.items()]
        request_batches = self.batch_requests(request_list, batch_size)
        # print(request_batches)

        async def process_batch(batch):
            # Try to acquire semaphore with built-in retry/backoff
            if self.use_semaphore:
                success, identifier = await self.redis_service.acquire_semaphore(
                    name=semaphore_name, 
                    max_locks=max_concurrency,
                    lock_timeout=lock_timeout,
                    max_attempts=self.max_semaphore_attempts,
                    initial_backoff=initial_backoff,
                    max_backoff=timeout,
                    backoff_factor=2.0
                )
            else:
                success = True 
                identifier = None 

            if not success:
                raise SemaphoreException(f"Failed to acquire semaphore '{semaphore_name}' after maximum attempts")
            
            try:
                if type(batch) == dict:
                    response = await make_post_request(batch['request'].model_dump(), url, 
                                                       timeout, retries, retry_sleep=1.0, log_id=log_id)
                    batch['response'] = response 
                else:
                    response = await make_post_request([i['request'].model_dump() for i in batch], 
                                                       url, timeout, retries, retry_sleep=1.0, log_id=log_id)
                    for i, request in enumerate(batch):
                        request['response'] = response[i]

                return batch 
            except Exception as e:
                print(f"{self.log_id}: Post request to {url} failed - {str(e)}")
                return []
            finally:
                if self.use_semaphore:
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

