from typing import List, Tuple, Optional, Dict

from vvs_database.execution.connections import Connections
from vvs_database.execution.execute import PluginExecutorFactory
from vvs_database.schemas import (
    ExecuteRequestUnion,
    ExecuteResponseUnion,
    ExecutePlugin,
)
from vvs_database.schemas.internal_schemas import ExecutionLog

class ExecutionOp():
    def __init__(self, 
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.connections = connections
        self.log_id = log_id
        self.execution_logs: dict[int, ExecutionLog] = {}

    def save_execution_log(self, execution_log: ExecutionLog):
        pid = execution_log.plugin_id
        if pid in self.execution_logs:
            self.execution_logs[pid].merge_from(execution_log)
        else:
            self.execution_logs[pid] = execution_log

    def reset_execution_log(self):
        self.execution_logs: dict[int, ExecutionLog] = {}

    def collect_execution_logs(self) -> dict[int, ExecutionLog]:
        """Return **all** logs produced by this op itself (no sub-ops)."""
        return {k:v.model_dump() for k,v in self.execution_logs.items()}
        
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
        self.save_execution_log(executor.execution_log)
        self.execution_log = executor.execution_log
        responses, checkin_response, valid_execution = res
        return responses, checkin_response, valid_execution
