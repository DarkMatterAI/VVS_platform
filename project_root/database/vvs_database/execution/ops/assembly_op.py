from typing import List, Dict, Optional 
import itertools 

from vvs_database.schemas.execute_schemas import (
    AssemblyRequest, 
    DataSourceResponse,
    Embedding,
    ItemData
)
from vvs_database.schemas.internal_schemas import (
    AssemblyItemInternal, 
    ExecutePlugin,
    InternalItem,
    InternalAssemblyData
)
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops.execution_op import ExecutionOp

def data_source_embedding_request_response_to_assembly_item(embedding: Embedding,
                                                            response: DataSourceResponse,
                                                            assembly_index: int
                                                           ) -> List[AssemblyItemInternal]:
    if not response.valid:
        return []
    
    assembly_items = []
    for result in response.result:
        embedding = Embedding(plugin_id=embedding.plugin_id,
                              plugin_name=embedding.plugin_name,
                              embedding=result.embedding)
        assembly_item = AssemblyItemInternal(item_id=result.item_id,
                                             external_id=result.external_id,
                                             item=result.item,
                                             assembly_index=assembly_index,
                                             embedding=embedding)
        assembly_items.append(assembly_item)
    return assembly_items

def build_assembly_requests(assembly_pools: Dict[int, List[AssemblyItemInternal]],
                            assembly_config: ExecutePlugin
                           ) -> List[AssemblyRequest]:
    requests = []
    for parents in itertools.product(*assembly_pools.values()):
        parents = list(parents)
        parents = sorted(parents, key=lambda x: x.assembly_index)
        request = AssemblyRequest(request_data=assembly_config.plugin.get_request_data(),
                                  parents=parents,
                                  runtime_args=assembly_config.runtime_args)
        requests.append(request)
    return requests 

class AssemblyOp(ExecutionOp):
    def __init__(self,
                 assembly_config: ExecutePlugin,
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.assembly_config = assembly_config
        self.connections = connections
        self.log_id = log_id
        
    async def __call__(self, 
                       request_dict: Dict[int, List[Embedding]],
                       response_dict: Dict[int, List[DataSourceResponse]]
                      ) -> List[InternalItem]:
        n_queries = len(next(iter(request_dict.values())))

        assembly_requests = []
        assembly_groups = []

        for query_group in range(n_queries):
            assembly_pools = {}
            for assembly_index in request_dict.keys():
                assembly_pools[assembly_index] = []
                request = request_dict[assembly_index][query_group]
                response = response_dict[assembly_index][query_group]
                assembly_items = data_source_embedding_request_response_to_assembly_item(request,
                                                                                         response,
                                                                                         assembly_index)
                assembly_pools[assembly_index] = assembly_items

            assembly_group_requests = build_assembly_requests(assembly_pools, self.assembly_config)
            assembly_requests += assembly_group_requests
            assembly_groups += [query_group for i in assembly_group_requests]
                
        assembly_responses, _, _ = await self.execute_plugin(assembly_requests, self.assembly_config)
        
        items = []
        for group, request, response in zip(assembly_groups, assembly_requests, assembly_responses):
            if not response.valid:
                continue 
                
            for result in response.result:
                parents = request.parents
                item_data = ItemData(item_id=result.item_id,
                                     external_id=str(result.external_id),
                                     item=result.item)
                item = InternalItem(item_data=item_data,
                                    score=None,
                                    embeddings={},
                                    assembly_data=InternalAssemblyData(assembly_id=result.assembly_id,
                                                                       parents=parents),
                                    query_group=group)
                items.append(item)
        return items
