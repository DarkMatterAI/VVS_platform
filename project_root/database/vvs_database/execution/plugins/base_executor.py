import uuid
from typing import List, Tuple, Dict, Optional, Any, Type

from vvs_database.schemas import ExecuteRequestUnion, ExecuteResponseUnion
from vvs_database.models import Plugin

from vvs_database.execution.redis import RedisService
from vvs_database.execution.db_service import DatabaseService
from vvs_database.execution.execution_strategy import APIExecutionStrategy, QueueExecutionStrategy

class BasePluginExecutor:
    """Base class for all plugin executors"""
    
    # Class variables for subclass configuration
    request_model: Type = None
    response_model: Type = None
    
    def __init__(self,
                 plugin: Plugin,
                 db_service: DatabaseService,
                 redis_service: RedisService,
                 db_lookup: bool=False,
                 db_persist: bool=False,
                 use_semaphore: bool=True,
                 max_semaphore_attempts: int=20,
                 queue_polling_interval: float=0.2,
                 ):
        self.plugin = plugin
        self.db_service = db_service
        self.redis_service = redis_service
        self.db_lookup = db_lookup
        self.db_persist = db_persist 
        self.use_semaphore = use_semaphore
        self.max_semaphore_attempts = max_semaphore_attempts
        self.queue_polling_interval = queue_polling_interval
        self.log_id = ''
        
        self._init_execution_strategy()
    
    def _init_execution_strategy(self):
        """Initialize the appropriate execution strategy"""
        if self.plugin.execution_type == 'api':
            self.execution_strategy = APIExecutionStrategy(
                self.redis_service,
                self.use_semaphore,
                self.max_semaphore_attempts
            )
        else:
            self.execution_strategy = QueueExecutionStrategy(
                self.redis_service,
                self.use_semaphore,
                self.max_semaphore_attempts,
                self.queue_polling_interval
            )

        if self.plugin.type.lower() == 'score':
            self.db_persist = True 
    
    def init_log_id(self, log_id: Optional[str]=None):
        """Initialize the logging ID for this execution"""
        if log_id is None:
            log_id = str(uuid.uuid4())

        self.log_id = log_id 
        self.db_service.log_id = f"{self.log_id}:DB"
        self.redis_service.log_id = f"{self.log_id}:Redis"
        self.execution_strategy.log_id = f"{self.log_id}:Execute {self.plugin.execution_type}"

    async def close(self):
        """Close all resources"""
        await self.redis_service.close()
        await self.execution_strategy.close()

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
        """Get a unique identifier from the request - can be overridden by subclasses"""
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

    # def generate_key(self, request: ExecuteRequestUnion) -> str:
    #     """Generate cache/dedupe key for a request - must be implemented by subclasses"""
    #     raise NotImplementedError("Subclasses must implement generate_key method")

    def deduplicate(self, 
                    requests: List[ExecuteRequestUnion]
                    ) -> Tuple[Dict[str, ExecuteRequestUnion], Dict[str, List[int]]]:
        """Deduplicate requests based on their keys"""
        print(f"{self.log_id}: Deduplicating {len(requests)} requests")
        keys = [r.generate_key(plugin_id=self.plugin.id) for r in requests]
        # keys = [self.generate_key(r) for r in requests]
        key_to_request = {}
        key_to_index = {}  # Maps keys to their original indices
        
        for i, (request, key) in enumerate(zip(requests, keys)):
            if key not in key_to_request:
                key_to_request[key] = request
            key_to_index.setdefault(key, []).append(i)

        print(f"{self.log_id}: {len(key_to_request.keys())} requests after deduplication")
        
        return key_to_request, key_to_index
    
    async def check_records(self, 
                            key_to_request: Dict[str, ExecuteRequestUnion]
                            ) -> Tuple[Dict[str, ExecuteResponseUnion],
                                       Dict[str, ExecuteResponseUnion],
                                       Dict[str, ExecuteRequestUnion]]:
        """Check request against cache and database"""
        print(f"{self.log_id}: Checking records with {len(key_to_request.keys())} keys")
        unique_keys = list(key_to_request.keys())

        # Check cache for records
        print(f"{self.log_id}: Checking cache")
        cached_results = await self.redis_service.get_cache_results(unique_keys)
        cached_results = {k: self.response_model.model_validate(v) 
                          for k, v in cached_results.items()}
        
        uncached_keys = [k for k in unique_keys if k not in cached_results]
        uncached_requests = {k: key_to_request[k] for k in uncached_keys}

        print(f"{self.log_id}: {len(uncached_requests.keys())} keys remain after cache, checking DB")

        # Check database for records
        db_results = {}
        if self.db_lookup:
            db_results = await self.db_service.query_database(self.plugin, uncached_requests)
            db_results = {k: self.response_model.model_validate(v) 
                          for k, v in db_results.items()}
        
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
        if remaining_requests:
            executed_results = await self.execution_strategy.execute(self.plugin, remaining_requests)
            executed_results = {k: self.response_model.model_validate(v)
                                for k, v in executed_results.items()}            
            await self.redis_service.set_results(executed_results)

        return executed_results 
    
    def reassemble_results(self, 
                           original_requests: List[ExecuteRequestUnion], 
                           results: Dict[str, ExecuteResponseUnion], 
                           key_to_index: Dict[str, List[int]]
                           ) -> List[ExecuteResponseUnion]:
        """Reassemble results in original order"""
        reassembled = [None] * len(original_requests)
        
        for key, indices in key_to_index.items():
            if key in results:
                result = results[key]
                for idx in indices:
                    reassembled[idx] = result
        
        # Fill any missing results with a failed response
        for i, result in enumerate(reassembled):
            if result is None:
                reassembled[i] = self.response_model.failure_response()
        
        return reassembled
    
    async def check_in_results(self, requests: List[ExecuteRequestUnion], 
                               results: List[ExecuteResponseUnion]) -> Any:
        """Check in results to the database - must be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement check_in_results method")

    async def execute(self, requests: List[ExecuteRequestUnion], log_id: Optional[str]=None):
        """Main execution flow that orchestrates the execution process"""
        if not requests:
            return [], None
        
        self.init_log_id(log_id)
        
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
        final_results = self.reassemble_results(requests, all_results, key_to_index)

        # Step 7: Check in items if needed
        # checkin_results = None
        # if self.db_persist:
        checkin_results = await self.check_in_results(requests, final_results)
        
        return final_results, checkin_results
