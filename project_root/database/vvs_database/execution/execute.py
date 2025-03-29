from typing import Union, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.utils import get_plugin_response_model
from vvs_database.execution.connections import get_connections
from vvs_database.schemas import BatchExecuteRequestUnion, ExecuteRequestUnion, ExecuteParams
from vvs_database.execution.plugins.executor_factory import PluginExecutorFactory

async def execute_plugin(db: AsyncSession, 
                         plugin_id: int, 
                         execute_request: Union[ExecuteRequestUnion, BatchExecuteRequestUnion],
                         execute_params: ExecuteParams,
                         return_all: bool=False,
                         log_id: Optional[str]=None
                         ):
    """
    Execute a plugin and optionally check in the results to the database.
    
    Args:
        db: Database session
        plugin_id: ID of the plugin to execute
        execute_request: Request data for the plugin
        execute_params: Execution parameters 
        return_all: Return checkin/valid execution data
        log_id: Log id for execution
        
    Returns:
        The plugin execution response
    """

    # Handle single request vs. batch
    delist = False 
    if not isinstance(execute_request, list):
        execute_request = [execute_request]
        delist = True 

    connections = get_connections(db)
    plugin_record = await connections.db_service.get_plugin(plugin_id)
    plugin = get_plugin_response_model(plugin_record)
    
    # Create appropriate executor using factory
    executor = PluginExecutorFactory.create_executor(
        plugin,
        connections,
        execute_params
    )
    
    # Execute the plugin
    response, checkin_response, valid_execution = await executor.execute(execute_request, log_id=log_id)
    
    # Clean up resources
    await executor.close()

    # Return single response if input was single
    if delist:
        response = response[0]

    if return_all:
        return response, checkin_response, valid_execution
        
    return response

