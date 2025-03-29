from typing import Dict 

from vvs_database.schemas import (
    ExecuteRequestUnion, 
    ExecuteResponseUnion, 
    ExecuteParams,
    PluginInDB
)
from vvs_database.execution.connections import Connections

class ExecutionStrategy:
    """Base class for plugin execution strategies"""

    def __init__(self, 
                 connections: Connections,
                 execute_params: ExecuteParams):
        self.log_id = 'Execute'
    
    async def execute(self, 
                      plugin: PluginInDB, 
                      requests: Dict[str, ExecuteRequestUnion]
                      ) -> Dict[str, ExecuteResponseUnion]:
        """Execute plugin with the given request data"""
        raise NotImplementedError("Subclasses must implement execute method")
    
