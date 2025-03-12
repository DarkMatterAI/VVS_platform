from typing import Optional, Union 

from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.execution.db_service import DatabaseService
from vvs_database.execution.redis import RedisService
from vvs_database.schemas import BatchExecuteRequestUnion, ExecuteRequestUnion
from vvs_database.execution.plugins.executor_factory import PluginExecutorFactory

class PluginExecutor:
    """Service for executing plugins with caching and concurrency control"""

    def __init__(self, db: AsyncSession):
        self.db_service = DatabaseService(db)

    async def execute_plugin(self, 
                             plugin_id: int, 
                             requests: BatchExecuteRequestUnion,
                             redis_url: Optional[str] = None,
                             cache_ttl: Optional[int] = None,
                             cache: bool = False,
                             db_lookup: bool = False,
                             db_persist: bool = False,
                             use_semaphore: bool = True,
                             max_semaphore_attempts: int = 20,
                             queue_polling_interval: float = 0.2,
                             ):
        """Execute a plugin with the given requests"""
        # Get the plugin from the database
        plugin = await self.db_service.get_plugin(plugin_id)
        
        # Initialize Redis service
        redis_service = RedisService(redis_url, cache_ttl, cache)
        
        # Create appropriate executor using factory
        executor = PluginExecutorFactory.create_executor(
            plugin,
            self.db_service,
            redis_service,
            db_lookup,
            db_persist,
            use_semaphore,
            max_semaphore_attempts,
            queue_polling_interval
        )
        
        # Execute the plugin
        response, checkin_response, valid_execution = await executor.execute(requests)
        
        # Clean up resources
        await executor.close()
        
        return response, checkin_response, valid_execution

async def execute_plugin(db: AsyncSession, 
                         plugin_id: int, 
                         execute_request: Union[ExecuteRequestUnion, BatchExecuteRequestUnion],
                         cache: bool = False,
                         db_lookup: bool = False,
                         db_persist: bool = False,
                         use_semaphore: bool = True,
                         max_semaphore_attempts: int = 20,
                         queue_polling_interval: float = 0.2,
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
        use_semaphore: Use semaphore to limit concurrency
        max_semaphore_attempts: Max number of attempts at acquiring semaphore
        queue_polling_interval: Polling interval for queue execution
        
    Returns:
        The plugin execution response
    """
    # Handle single request vs. batch
    delist = False 
    if not isinstance(execute_request, list):
        execute_request = [execute_request]
        delist = True 

    # Execute the plugin
    executor = PluginExecutor(db)
    response, _, _ = await executor.execute_plugin(
        plugin_id, 
        execute_request, 
        cache=cache,
        db_lookup=db_lookup,
        db_persist=db_persist,
        use_semaphore=use_semaphore,
        max_semaphore_attempts=max_semaphore_attempts,
        queue_polling_interval=queue_polling_interval
    )

    # Return single response if input was single
    if delist:
        response = response[0]
        
    return response

