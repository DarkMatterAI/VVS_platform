from typing import List, Dict, Optional 
from collections import defaultdict

from vvs_database.schemas.execute_schemas import (
    Embedding,
    MapperRequest
)
from vvs_database.schemas.internal_schemas import ExecutePlugin, ExecutionLog
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops.execution_op import ExecutionOp

def embedding_to_mapper_request(embeddings: List[Embedding],
                                plugin_config: ExecutePlugin
                               ) -> List[MapperRequest]:
    requests = [MapperRequest(request_data=plugin_config.plugin.get_request_data(),
                              embedding=embedding,
                              runtime_args=plugin_config.runtime_args)
                for embedding in embeddings]
    return requests

class MapperOp(ExecutionOp):
    def __init__(self,
                 mapper_config: ExecutePlugin,
                 embedding_configs: List[ExecutePlugin],
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.mapper_config = mapper_config
        self.embedding_configs = embedding_configs
        self.connections = connections 
        self.log_id = log_id
        self.embedding_order = {i.index:i.embedding_id 
                                for i in mapper_config.plugin.output_order}
        self.embedding_dict = {i.plugin.id:i.plugin for i in embedding_configs}
        self.execution_logs: dict[int, ExecutionLog] = {}
        
    async def __call__(self,
                       requests: List[Embedding]
                      ) -> Dict[int, List[Embedding]]:
        requests = embedding_to_mapper_request(requests, self.mapper_config)
        responses, _, _ = await self.execute_plugin(requests, self.mapper_config)
        
        response_dict = defaultdict(list)
        for response in responses:
            if not response.valid:
                continue
                
            for assembly_index, embedding in enumerate(response.embedding):
                embedding_plugin = self.embedding_dict[self.embedding_order[assembly_index]]
                embedding = Embedding(plugin_id=embedding_plugin.id,
                                      plugin_name=embedding_plugin.name,
                                      embedding=embedding)
                response_dict[assembly_index].append(embedding)
        return response_dict 