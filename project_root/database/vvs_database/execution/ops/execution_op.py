from typing import List, Tuple, Optional, Dict

from vvs_database.execution.connections import Connections
from vvs_database.execution.execute import PluginExecutorFactory
from vvs_database.schemas import (
    ExecuteRequestUnion,
    ExecuteResponseUnion,
    ExecutePlugin,
)

class ExecutionOp():
    def __init__(self, 
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.connections = connections
        self.log_id = log_id
        
    async def execute_plugin(self, 
                             requests: List[ExecuteRequestUnion], 
                             plugin_config: ExecutePlugin
                            ) -> Tuple[List[ExecuteResponseUnion], Optional[Dict], List[bool]]:
        if not requests:
            return [], None, []
            
        executor = PluginExecutorFactory.create_executor(plugin_config.plugin,
                                                         self.connections,
                                                         plugin_config.execute_params)
        
        res = await executor.execute(requests, log_id=self.log_id)
        self.execution_log = executor.execution_log
        responses, checkin_response, valid_execution = res
        return responses, checkin_response, valid_execution
