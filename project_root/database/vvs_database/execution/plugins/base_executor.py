import uuid
import json 
import asyncio 
from typing import List, Tuple, Dict, Optional, Any, Type

from vvs_database.schemas import ExecuteRequestUnion, ExecuteResponseUnion, ExecuteParams
from vvs_database.models import Plugin

from vvs_database.execution.connections import Connections
from vvs_database.execution.execution_strategy import APIExecutionStrategy, QueueExecutionStrategy

class BasePluginExecutor:
    """Base class for all plugin executors"""
    
    # Class variables for subclass configuration
    request_model: Type = None
    response_model: Type = None
    
    def __init__(self,
                 plugin: Plugin,
                 connections: Connections,
                 execute_params: ExecuteParams):
        
        self._init_execution_strategy(plugin, connections, execute_params)
    
    def _init_execution_strategy(self,
                                 plugin: Plugin,
                                 connections: Connections,
                                 execute_params: ExecuteParams):
        """Initialize the appropriate execution strategy"""
        self.log_id = ''
        self.plugin = plugin
        self.connections = connections 
        self.execute_params = self.update_params(execute_params)
        if self.plugin.execution_type == 'api':
            self.execution_strategy = APIExecutionStrategy(
                self.connections,
                self.execute_params
            )
        else:
            self.execution_strategy = QueueExecutionStrategy(
                self.connections,
                self.execute_params
            )

    def update_params(self, execute_params: ExecuteParams):
        return execute_params 
    
    def init_log_id(self, log_id: Optional[str]=None):
        """Initialize the logging ID for this execution"""
        if log_id is None:
            log_id = str(uuid.uuid4())

        self.log_id = log_id 
        self.connections.init_log_id(self.log_id)
        self.execution_strategy.log_id = f"{self.log_id}:Execute {self.plugin.execution_type}"

    async def close(self):
        """Close all resources"""
        await self.connections.close()

    def populate_request_id(self, request: ExecuteRequestUnion) -> ExecuteRequestUnion:
        """Add request_id to request data if not present"""
        if request.request_data.request_id is None:
            group_key = self.plugin.group_key 
            plugin_type = self.plugin.type 
            plugin_id = self.plugin.id  
            request_id = str(uuid.uuid4())

            unique_id = self._get_request_unique_id(request)

            request_key = f"request.{group_key}.{plugin_type}.{plugin_id}.{unique_id}.{request_id}"
            request.request_data.request_id = request_key 
            request.request_data.plugin_id = self.plugin.id
            request.request_data.plugin_name = self.plugin.name
        return request
    
    def _get_request_unique_id(self, request: ExecuteRequestUnion) -> str:
        """Get a unique identifier from the request"""
        if hasattr(request, 'item_data'):
            return str(request.item_data.item_id)
        return str(uuid.uuid4())
        
    def validate_requests(self, 
                          requests: List[ExecuteRequestUnion]
                          ) -> List[ExecuteRequestUnion]:
        """Validate request schema"""
        print(f"{self.log_id}: Validating {len(requests)} requests")
        processed_requests = []
        for request in requests:
            request = self.request_model.model_validate(request)
            request = self.populate_request_id(request)
            processed_requests.append(request)
        return processed_requests

    def deduplicate(self, 
                    requests: List[ExecuteRequestUnion]
                    ) -> Tuple[Dict[str, ExecuteRequestUnion], Dict[str, List[int]]]:
        """Deduplicate requests based on their keys"""
        print(f"{self.log_id}: Deduplicating {len(requests)} requests")
        keys = [r.generate_key(plugin_id=self.plugin.id) for r in requests]
        key_to_request = {}
        key_to_index = {}  # Maps keys to their original indices
        
        for i, (request, key) in enumerate(zip(requests, keys)):
            if key not in key_to_request:
                key_to_request[key] = request
            key_to_index.setdefault(key, []).append(i)

        print(f"{self.log_id}: {len(key_to_request.keys())} requests after deduplication")
        
        return key_to_request, key_to_index
    
    async def get_cache_results(self, keys: List[str]) -> Dict[str, Any]:
        if (not self.execute_params.cache) or (not keys):
            return {}
        
        results = await self.connections.redis_service.get_results(keys)
        return results 
    
    async def check_records(self, 
                            key_to_request: Dict[str, ExecuteRequestUnion]
                            ) -> Tuple[Dict[str, ExecuteResponseUnion],
                                       Dict[str, ExecuteResponseUnion],
                                       Dict[str, ExecuteRequestUnion]]:
        """Check request against cache and database"""
        unique_keys = list(key_to_request.keys())
        n_keys = len(unique_keys)
        print(f"{self.log_id}: Checking records with {n_keys} keys")

        # Check cache for records
        print(f"{self.log_id}: Checking cache")
        cached_results = await self.get_cache_results(unique_keys)
        cached_results = {k: self.response_model.model_validate(v) 
                          for k, v in cached_results.items()}
        
        uncached_keys = [k for k in unique_keys if k not in cached_results]
        uncached_requests = {k: key_to_request[k] for k in uncached_keys}

        print(f"{self.log_id}: {len(uncached_requests.keys())} keys remain after cache, checking DB")

        # Check database for records
        db_results = await self.query_database(self.plugin, uncached_requests)
        
        # Determine remaining requests to execute
        remaining_keys = [k for k in uncached_keys if k not in db_results]
        remaining_requests = {k: key_to_request[k] for k in remaining_keys}

        print(f"{self.log_id}: {len(remaining_requests.keys())} keys remain after DB")
        return cached_results, db_results, remaining_requests

    async def execute_plugin(self, 
                             remaining_requests: Dict[str, ExecuteRequestUnion]
                             ) -> Dict[str, ExecuteResponseUnion]:
        """Execute plugin request. Optionally cache/persist results"""
        print(f"{self.log_id}: Executing plugin with {len(remaining_requests.keys())} requests")
        executed_results = {}
        if not remaining_requests:
            return executed_results

        raw_result = await self.execution_strategy.execute(self.plugin, remaining_requests)
        failed_execution = []
        for k,v in raw_result.items():
            if v["valid"]:
                try:
                    response_data = self.response_model.model_validate(v["response_data"])
                    executed_results[k] = response_data
                except Exception as e:
                    v["valid"] = False 
                    v["failure_reasion"] = f"Model validation error: {self.plugin.type}"
                    v["failure_detail"] = f"Response: {json.dumps(v['response_data'])}, Error: {str(e)}"

            if not v["valid"]:
                print(f"{self.log_id}: Failed execution: plugin {self.plugin.id}, " \
                        f"{v['failure_reason']}, {v['failure_detail']}")
                failed_execution.append((remaining_requests[k], v))

        await self.connections.db_service.log_failed_requests(self.plugin, failed_execution)

        if self.execute_params.cache:
            await self.connections.redis_service.set_results(executed_results)

        return executed_results 
    
    def reassemble_results(self, 
                           original_requests: List[ExecuteRequestUnion], 
                           results: Dict[str, ExecuteResponseUnion], 
                           key_to_index: Dict[str, List[int]]
                           ) -> Tuple[List[ExecuteResponseUnion], List[bool]]:
        """Reassemble results in original order"""
        reassembled = [None] * len(original_requests)
        valid_execution = [True] * len(original_requests)
        
        for key, indices in key_to_index.items():
            if key in results:
                result = results[key]
                for idx in indices:
                    reassembled[idx] = result
        
        # Fill any missing results with a failed response
        for i, result in enumerate(reassembled):
            if result is None:
                reassembled[i] = self.response_model.failure_response()
                valid_execution[i] = False
        
        return reassembled, valid_execution
    
    async def check_in_results(self, 
                               requests: List[ExecuteRequestUnion], 
                               results: List[ExecuteResponseUnion],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in results to the database - must be implemented by subclasses"""
        await asyncio.sleep(0)
        return None 
    
    async def query_database(self, 
                             plugin: Plugin, 
                             requests: Dict[str, ExecuteRequestUnion]
                             ) -> Dict[str, ExecuteResponseUnion]:
        """Query database for existing results - must be implemented by subclasses"""
        result = {}
        await asyncio.sleep(0)
        return result 

    async def execute(self, requests: List[ExecuteRequestUnion], log_id: Optional[str]=None):
        """Main execution flow that orchestrates the execution process"""
        self.init_log_id(log_id)

        if not requests:
            print(f"{self.log_id}: No requests - returning")
            return [], None, []

        print(f"{self.log_id}: Executing {len(requests)} requests for plugin {self.plugin.id}")
        
        # Step 1: Validate requests
        requests = self.validate_requests(requests)

        # Step 2: Generate keys and deduplicate
        key_to_request, key_to_index = self.deduplicate(requests)
        
        # Step 3: Check cache and database for existing results
        (cached_results, 
         db_results, 
         remaining_requests) = await self.check_records(key_to_request)
        
        # Step 4: Execute plugin for remaining requests
        executed_results = await self.execute_plugin(remaining_requests)

        # Step 5: Combine all results
        all_results = {**cached_results, **db_results, **executed_results}
        
        # Step 6: Reassemble results in original order
        final_results, valid_execution = self.reassemble_results(requests, all_results, key_to_index)

        # Step 7: Check in items if needed
        checkin_results = await self.check_in_results(requests, final_results, valid_execution)
        
        return final_results, checkin_results, valid_execution
