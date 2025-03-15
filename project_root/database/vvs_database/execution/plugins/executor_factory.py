from vvs_database.schemas import PluginType
from vvs_database.models import Plugin
from vvs_database.execution.connections import DatabaseService, RedisService, RabbitMQService, Connections
from vvs_database.execution.plugins.base_executor import BasePluginExecutor
from vvs_database.execution.plugins.plugin_executors import EXECUTOR_DICT

class PluginExecutorFactory:
    """Factory to create the appropriate plugin executor"""
    
    @staticmethod
    def create_executor(
            plugin: Plugin,
            connections: Connections,
            cache: bool=False,
            db_lookup: bool = False,
            db_persist: bool = False,
            use_semaphore: bool = True,
            max_semaphore_attempts: int = 20,
            queue_polling_interval: float = 0.2
        ) -> BasePluginExecutor:
        """Create the appropriate plugin executor based on plugin type"""
        
        plugin_type = plugin.type.lower()
        executor_class = EXECUTOR_DICT.get(plugin_type)
        if executor_class is None:
            raise ValueError(f"Unknown plugin type: {plugin_type}")
        executor = executor_class(plugin, 
                                  connections,
                                  cache,
                                  db_lookup, 
                                  db_persist,
                                  use_semaphore, 
                                  max_semaphore_attempts, 
                                  queue_polling_interval)
        return executor 
