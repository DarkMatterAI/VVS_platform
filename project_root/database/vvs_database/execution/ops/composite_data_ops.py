from typing import List, Dict, Optional 

from vvs_database.schemas.execute_schemas import (
    DataSourceResponse,
    Embedding,
    ItemData
)
from vvs_database.schemas.internal_schemas import (
    ExecutePlugin,
    InternalItem,
    Query,
    ExecuteDataSource
)
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops.execution_op import ExecutionOp
from vvs_database.execution.ops.data_op import DataOp
from vvs_database.execution.ops.assembly_op import AssemblyOp
from vvs_database.execution.ops.mapper_op import MapperOp
from vvs_database.execution.ops.item_op import ItemOp

def data_source_embedding_request_response_to_item(requests: List[Embedding],
                                                   responses: List[DataSourceResponse]
                                                  ) -> List[InternalItem]:
    items = []
    for query_group, (embedding, response) in enumerate(zip(requests, responses)):
        if not response.valid:
            continue 
            
        for result in response.result:
            embedding_id = embedding.plugin_id
            embedding = Embedding(plugin_id=embedding_id,
                                  plugin_name=embedding.plugin_name,
                                  embedding=result.embedding)
            item_data = ItemData(item_id=result.item_id,
                                 external_id=str(result.external_id),
                                 item=result.item)
            item = InternalItem(item_data=item_data,
                                valid=True,
                                score=None,
                                embeddings={embedding_id : embedding},
                                assembly_data=None,
                                query_group=query_group,
                                update_embedding=embedding)
            items.append(item)
    return items

class SingleDataOp(ExecutionOp):
    def __init__(self,
                 data_config: ExecuteDataSource,
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.data_op = DataOp({0:data_config}, connections, log_id)
        self.connections = connections
        self.log_id = log_id
        
    async def __call__(self,
                       query: Query
                      ) -> List[InternalItem]:
        request_dict = {0 : query.to_embeddings()}
        response_dict = await self.data_op(request_dict)
        requests = request_dict[0]
        responses = response_dict[0]
        items = data_source_embedding_request_response_to_item(requests, responses)
        return items

class DecomposedDataOp(ExecutionOp):
    def __init__(self,
                 data_config_dict: Dict[int, ExecuteDataSource],
                 assembly_config: ExecutePlugin,
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.data_keys = data_config_dict.keys()
        self.data_op = DataOp(data_config_dict, connections, log_id)
        self.assembly_op = AssemblyOp(assembly_config, connections, log_id)
        self.log_id = log_id
        
    async def __call__(self,
                       query: Query
                      ) -> List[InternalItem]:
        request_dict = query.to_embedding_dict()
        assert request_dict.keys() == self.data_keys
        response_dict = await self.data_op(request_dict)
        items = await self.assembly_op(request_dict, response_dict)
        return items

class MapperDataOp(ExecutionOp):
    def __init__(self,
                 mapper_config: ExecutePlugin,
                 input_embedding_config: ExecutePlugin,
                 output_embedding_configs: List[ExecutePlugin],
                 data_config_dict: Dict[int, ExecuteDataSource],
                 assembly_config: ExecutePlugin,
                 connections: Connections,
                 log_id: Optional[str]=None):
        
        self.mapper_op = MapperOp(mapper_config, output_embedding_configs, connections, log_id)
        self.data_op = DataOp(data_config_dict, connections, log_id)
        self.assembly_op = AssemblyOp(assembly_config, connections, log_id)
        self.embedding_op = ItemOp(input_embedding_config, [], connections, log_id)
        self.update_embedding_id = input_embedding_config.plugin.id
        
    async def __call__(self,
                       query: Query
                      ) -> List[InternalItem]:
        requests = query.to_embeddings()
        request_dict = await self.mapper_op(requests)
        query.update_from_mapper(request_dict)
        response_dict = await self.data_op(request_dict)
        items = await self.assembly_op(request_dict, response_dict)
        items = await self.embedding_op(items)
        for item in items:
            item.update_embedding = item.embeddings.get(self.update_embedding_id)
        return items
