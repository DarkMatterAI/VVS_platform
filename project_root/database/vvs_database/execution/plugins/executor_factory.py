from vvs_database.schemas import PluginType, ExecuteParams
from vvs_database.models import Plugin
from vvs_database.execution.connections import Connections
from vvs_database.execution.plugins.base_executor import BasePluginExecutor
from vvs_database.execution.plugins.plugin_executors import EXECUTOR_DICT

class PluginExecutorFactory:
    """Factory to create the appropriate plugin executor"""
    
    @staticmethod
    def create_executor(
            plugin: Plugin,
            connections: Connections,
            execute_params: ExecuteParams,
        ) -> BasePluginExecutor:
        """Create the appropriate plugin executor based on plugin type"""
        
        plugin_type = plugin.type.lower()
        executor_class = EXECUTOR_DICT.get(plugin_type)
        if executor_class is None:
            raise ValueError(f"Unknown plugin type: {plugin_type}")
        executor = executor_class(plugin, 
                                  connections,
                                  execute_params)
        return executor 
