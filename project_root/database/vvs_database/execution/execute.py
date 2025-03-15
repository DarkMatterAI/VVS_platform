from typing import Optional, Union 

from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.execution.connections import get_connections, DatabaseService, RedisService, RabbitMQService
from vvs_database.schemas import (
    BatchExecuteRequestUnion, 
    ExecuteRequestUnion, 
    RedisConnection,
    RabbitMQConnection
)
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
    # Handle single request vs. batch
    delist = False 
    if not isinstance(execute_request, list):
        execute_request = [execute_request]
        delist = True 

    connections = get_connections(db)
    plugin = await connections.db_service.get_plugin(plugin_id)

    # Execute the plugin
    # db_service = DatabaseService(db)
    # plugin = await db_service.get_plugin(plugin_id)
    
    # Initialize Redis service
    # redis_connection = RedisConnection()
    # redis_service = RedisService(redis_connection)
    # redis_service = RedisService(redis_url=None, cache_ttl=None)

    # Initialize Rabbitmq service
    # rabbitmq_connection = RabbitMQConnection()
    # rabbitmq_service = RabbitMQService(rabbitmq_connection)
    
    # Create appropriate executor using factory
    executor = PluginExecutorFactory.create_executor(
        plugin,
        connections,
        # db_service,
        # redis_service,
        # rabbitmq_service,
        cache,
        db_lookup,
        db_persist,
        use_semaphore,
        max_semaphore_attempts,
        queue_polling_interval
    )
    
    # Execute the plugin
    response, checkin_response, valid_execution = await executor.execute(execute_request)
    
    # Clean up resources
    await executor.close()



    # executor = PluginExecutor(db)
    # response, checkin_response, valid_execution = await executor.execute_plugin(
    #     plugin_id, 
    #     execute_request, 
    #     cache=cache,
    #     db_lookup=db_lookup,
    #     db_persist=db_persist,
    #     use_semaphore=use_semaphore,
    #     max_semaphore_attempts=max_semaphore_attempts,
    #     queue_polling_interval=queue_polling_interval
    # )

    # Return single response if input was single
    if delist:
        response = response[0]

    if return_all:
        return response, checkin_response, valid_execution
        
    return response

