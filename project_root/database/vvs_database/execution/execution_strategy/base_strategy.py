from typing import Dict 
import uuid 
import asyncio 

from vvs_database.schemas import ExecuteRequestUnion, ExecuteResponseUnion
from vvs_database.models import Plugin 
from vvs_database.execution.redis import RedisService

class ExecutionStrategy:
    """Base class for plugin execution strategies"""

    def __init__(self, redis_service: RedisService):
        self.redis_service = redis_service 
        self.log_id = 'Execute'
    
    def populate_request_id(self, 
                            plugin: Plugin, 
                            request: ExecuteRequestUnion
                            ):
        """Add request_id to request data if not present"""

        if request.request_data.request_id is None:
            group_key = plugin.group_key 
            plugin_type = plugin.type 
            plugin_id = plugin.id  
            request_id = str(uuid.uuid4())

            if hasattr(request, 'item_data'):
                item_id = request.item_data.item_id
            else:
                item_id = str(uuid.uuid4())

            request_key = f"request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}"
            request.request_data.request_id = request_key 
        return request     

    async def close(self):
        await asyncio.sleep(0)
    
    async def execute(self, 
                      plugin: Plugin, 
                      requests: Dict[str, ExecuteRequestUnion]
                      ) -> Dict[str, ExecuteResponseUnion]:
        """Execute plugin with the given request data"""
        raise NotImplementedError("Subclasses must implement execute method")
    
