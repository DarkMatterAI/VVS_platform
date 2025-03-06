from typing import Optional, Union 

from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.execution.database import DatabaseService
from vvs_database.execution.redis import RedisService
from vvs_database.schemas import BatchExecuteRequestUnion, ExecuteRequestUnion
from vvs_database.execution.plugins.base_executor import BasePluginExecutor

class PluginExecutor:
    """Service for executing plugins with caching and concurrency control"""

    def __init__(self,
                 db: AsyncSession
                 ):
        self.db_service = DatabaseService(db)

    async def execute_plugin(self, 
                             plugin_id: int, 
                             requests: BatchExecuteRequestUnion,
                             redis_url: Optional[str] = None,
                             cache_ttl: Optional[int] = None,
                             cache: bool = False,
                             db_lookup: bool=False,
                             db_persist: bool = False
                             ):
        plugin = await self.db_service.get_plugin(plugin_id)

        redis_service = RedisService(redis_url, cache_ttl, cache)

        executor = BasePluginExecutor(plugin, self.db_service, redis_service, db_lookup, db_persist)
        response, checkin_response = await executor.execute(requests)
        await executor.close()
        return response, checkin_response 

async def execute_plugin(db: AsyncSession, 
                         plugin_id: int, 
                         execute_request: Union[ExecuteRequestUnion, BatchExecuteRequestUnion],
                         cache: bool=False,
                         db_lookup: bool=False,
                         db_persist: bool=False,
                         ):
    """
    Execute a plugin and optionally check in the results to the database.
    
    Args:
        db: Database session
        plugin_id: ID of the plugin to execute
        execute_request: Request data for the plugin
        cache: Whether to get/set from redis cache
        db_lookup: Whether to look up saved results before execution
        db_persist: Whether to check in the results to the database
        
    Returns:
        The plugin execution response
    """
    delist = False 
    if type(execute_request) != list:
        execute_request = [execute_request]
        delist = True 

    executor = PluginExecutor(db)
    response, _ = await executor.execute_plugin(plugin_id, 
                                                execute_request, 
                                                cache=cache,
                                                db_lookup=db_lookup,
                                                db_persist=db_persist,
                                                )

    if delist:
        response = response[0]
    return response 








# from typing import Dict, Type
# from sqlalchemy.ext.asyncio import AsyncSession

# class PluginExecutorFactory:
#     """Factory for creating plugin executors based on plugin type"""
    
#     def __init__(self, 
#                  db_service, 
#                  cache_service, 
#                  api_strategy, 
#                  queue_strategy):
#         self.db_service = db_service
#         self.cache_service = cache_service
#         self.api_strategy = api_strategy
#         self.queue_strategy = queue_strategy
        
#         # Register executor classes by plugin type
#         self.executors = {
#             'embedding': EmbeddingPluginExecutor,
#             'data_source': DataSourcePluginExecutor,
#             'filter': FilterPluginExecutor,
#             'score': ScorePluginExecutor,
#             'mapper': MapperPluginExecutor,
#             'assembly': AssemblyPluginExecutor
#         }
    
#     def get_executor(self, plugin_type: str) -> BasePluginExecutor:
#         """Get executor for a specific plugin type"""
#         executor_class = self.executors.get(plugin_type)
#         if not executor_class:
#             raise ValueError(f"No executor registered for plugin type: {plugin_type}")
        
#         return executor_class(
#             self.db_service,
#             self.cache_service,
#             self.api_strategy,
#             self.queue_strategy
#         )

# class PluginService:
#     """Service for executing plugins with caching and concurrency control"""
    
#     def __init__(self, db: AsyncSession, redis_url: str, rabbitmq_config: dict):
#         # Initialize services
#         self.db_service = DatabaseService(db)
#         self.cache_service = RedisCacheService(redis_url)
#         self.concurrency_manager = RedisConcurrencyManager(redis_url)
        
#         # Initialize strategies
#         self.api_strategy = APIExecutionStrategy(self.concurrency_manager)
#         self.queue_strategy = QueueExecutionStrategy(
#             self.concurrency_manager, 
#             redis_url, 
#             rabbitmq_config
#         )
        
#         # Initialize executor factory
#         self.executor_factory = PluginExecutorFactory(
#             self.db_service,
#             self.cache_service,
#             self.api_strategy,
#             self.queue_strategy
#         )
    
#     async def execute_plugin(self, plugin_id: int, requests, checkin_result: bool = True):
#         """Execute a plugin with the appropriate executor"""
#         try:
#             # Get the plugin from database
#             plugin = await self.db_service.get_plugin(plugin_id)
            
#             # Get appropriate executor for this plugin type
#             executor = self.executor_factory.get_executor(plugin.type)
            
#             # Execute the plugin
#             results = await executor.execute(plugin, requests)
            
#             return results
            
#         except Exception as e:
#             # Handle exceptions
#             if isinstance(requests, list):
#                 return [{'valid': False, 'failure_reason': str(e)}] * len(requests)
#             else:
#                 return {'valid': False, 'failure_reason': str(e)}
        
#     async def close(self):
#         """Close all connections"""
#         await self.cache_service.close()
#         await self.concurrency_manager.close()