from typing import List, Dict, Optional 

from vvs_database.schemas.execute_schemas import (
    DataSourceRequest, 
    DataSourceResponse,
    Embedding
)
from vvs_database.schemas.internal_schemas import ExecuteDataSource
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops.execution_op import ExecutionOp

def embedding_to_data_request(embeddings: List[Embedding],
                              plugin_config: ExecuteDataSource
                             ) -> List[DataSourceRequest]:
    requests = [DataSourceRequest(request_data=plugin_config.plugin.get_request_data(),
                                  embedding=embedding,
                                  k=plugin_config.data_source_params.k,
                                  runtime_args=plugin_config.runtime_args)
                for embedding in embeddings]
    return requests

class DataOp(ExecutionOp):
    def __init__(self, 
                 data_config_dict: Dict[int, ExecuteDataSource],
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.data_config_dict = data_config_dict
        self.connections = connections
        self.log_id = log_id
        
    async def __call__(self, 
                       request_dict: Dict[int, List[Embedding]]
                      ) -> Dict[int, DataSourceResponse]:
        response_dict = {}
        for assembly_index, embeddings in request_dict.items():
            data_config = self.data_config_dict[assembly_index]
            requests = embedding_to_data_request(embeddings, data_config)
            responses, _, _ = await self.execute_plugin(requests, data_config)
            response_dict[assembly_index] = responses

        return response_dict 
