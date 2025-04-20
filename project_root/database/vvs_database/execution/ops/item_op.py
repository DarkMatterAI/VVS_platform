from typing import Tuple, List, Optional 

from vvs_database.schemas import (
    InternalItem,
    ExecutePlugin,
    ItemRequest,
    ItemDataEmbed,
    Embedding,
    ScoreResult,
    ExecuteResponseUnion,
    PluginInDBUnion
)
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops.execution_op import ExecutionOp


def gather_valid_items(items: List[InternalItem]
                      ) -> Tuple[List[InternalItem], List[int]]:
    valid_items = []
    idxs = []
    for idx, item in enumerate(items):
        if item.valid:
            valid_items.append(item)
            idxs.append(idx)
    return valid_items, idxs

def item_to_item_request(items: List[InternalItem], 
                         plugin: PluginInDBUnion,
                         runtime_args: Optional[dict]=None,
                        ) -> List[ItemRequest]:
    embedding_ids = []
    if (plugin.type != 'embedding') and (plugin.embedding_ids is not None):
        embedding_ids = plugin.embedding_ids
    
    requests = []
    for item in items:
        embeddings = [item.embeddings[i] for i in embedding_ids]
        request_id = plugin.get_request_key(item.item_data.item_id)
        item_data = item.item_data.model_dump()
        item_data['embeddings'] = embeddings
        item_data = ItemDataEmbed(**item_data)
        request = ItemRequest(request_data=plugin.get_request_data(request_id),
                              item_data=item_data,
                              runtime_args=runtime_args)
        requests.append(request)
    return requests 

def item_response_scatter(items: List[InternalItem],
                          responses: List[ExecuteResponseUnion],
                          idxs: List[int],
                          plugin_config: ExecutePlugin
                         ) -> List[InternalItem]:
    plugin = plugin_config.plugin
    
    for idx, response in zip(idxs, responses):
        item = items[idx]
        item.valid = response.valid
        if not item.valid:
            continue 
            
        if plugin.type == 'score':
            item.score = ScoreResult(plugin_id=plugin.id,
                                     plugin_name=plugin.name, 
                                     score=response.score)
        elif plugin.type == 'embedding':
            item.embeddings[plugin.id] = Embedding(plugin_id=plugin.id,
                                                   plugin_name=plugin.name,
                                                   embedding=response.embedding)
    return items 

def check_has_embedding(items: List[InternalItem], 
                        plugin_config: ExecutePlugin
                       ) -> bool:
    for item in items:
        if plugin_config.plugin.id not in item.embeddings:
            return False
    return True 

class ItemOp(ExecutionOp):
    def __init__(self, 
                 plugin_config: ExecutePlugin,
                 embedding_configs: List[ExecutePlugin],
                 connections: Connections,
                 log_id: Optional[str]=None):
        self.plugin_config = plugin_config
        self.embedding_configs = embedding_configs
        self.connections = connections 
        self.log_id = log_id
        
    async def gather_execute_scatter(self, 
                                     items: List[InternalItem],
                                     plugin_config: ExecutePlugin,
                                    ) -> List[InternalItem]:
        valid_items, idxs = gather_valid_items(items)
        requests = item_to_item_request(valid_items, plugin_config.plugin, plugin_config.runtime_args)
        responses, _, _ = await self.execute_plugin(requests, plugin_config)
        items = item_response_scatter(items, responses, idxs, plugin_config)
        return items 
        
    async def __call__(self, 
                       items: List[InternalItem]
                      ) -> List[InternalItem]:
        for embedding_config in self.embedding_configs:
            if not check_has_embedding(items, embedding_config):
                items = await self.gather_execute_scatter(items, embedding_config)
        
        items = await self.gather_execute_scatter(items, self.plugin_config)
        return items 
        
