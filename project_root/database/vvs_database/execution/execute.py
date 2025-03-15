from typing import Union 

from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.execution.connections import get_connections
from vvs_database.schemas import BatchExecuteRequestUnion, ExecuteRequestUnion, ExecuteParams
from vvs_database.execution.plugins.executor_factory import PluginExecutorFactory

async def execute_plugin(db: AsyncSession, 
                         plugin_id: int, 
                         execute_request: Union[ExecuteRequestUnion, BatchExecuteRequestUnion],
                         cache: bool = False,
                         db_lookup: bool = False,
                         db_persist: bool = False,
                         use_semaphore: bool = True,
                         max_semaphore_attempts: int = 20,
                         queue_polling_interval: float = 0.2,
                         backoff_factor: float=2.0,
                         return_all: bool=False
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
        return_all: Return checkin/valid execution data
        
    Returns:
        The plugin execution response
    """

    execute_params = ExecuteParams(cache=cache,
                                   db_lookup=db_lookup,
                                   db_persist=db_persist,
                                   use_semaphore=use_semaphore,
                                   max_semaphore_attempts=max_semaphore_attempts,
                                   queue_polling_interval=queue_polling_interval,
                                   backoff_factor=backoff_factor)

    # Handle single request vs. batch
    delist = False 
    if not isinstance(execute_request, list):
        execute_request = [execute_request]
        delist = True 

    connections = get_connections(db)
    plugin = await connections.db_service.get_plugin(plugin_id)
    
    # Create appropriate executor using factory
    executor = PluginExecutorFactory.create_executor(
        plugin,
        connections,
        execute_params
    )
    
    # Execute the plugin
    response, checkin_response, valid_execution = await executor.execute(execute_request)
    
    # Clean up resources
    await executor.close()

    # Return single response if input was single
    if delist:
        response = response[0]

    if return_all:
        return response, checkin_response, valid_execution
        
    return response

