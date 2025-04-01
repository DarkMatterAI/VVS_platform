from pydantic import BaseModel, ConfigDict
from typing import List, Union, Optional, Dict

from vvs_database.schemas.plugin_schemas import PluginInDBUnion
from vvs_database.schemas.execute_schemas import (
    ExecuteParams,
    ItemData,
    Embedding,
    AssemblyItem
)

class PluginRecord(BaseModel):
    plugin: Optional[PluginInDBUnion]=None 

class ExecuteDataParams(BaseModel):
    k: int 
    assembly_index: int

class ExecutePluginParams(BaseModel):
    plugin_id: int
    execute_params: ExecuteParams
    runtime_args: Optional[dict]=None

class ExecutePluginCreate(ExecutePluginParams):
    # create - plugin params
    pass 

class ExecuteDataSourceCreate(ExecutePluginParams, ExecuteDataParams):
    # create - plugin params and data params
    pass 

class ExecutePlugin(ExecutePluginCreate, PluginRecord):
    # internal - plugin params, plugin
    pass 

class ExecuteDataSource(ExecuteDataSourceCreate, PluginRecord):
    # internal - plugin params, plugin, data params
    pass 

class Parent(BaseModel):
    item_data: ItemData
    embedding: Embedding
    assembly_index: int 

class AssemblyItemInternal(AssemblyItem):
    embedding: Embedding
        
    def to_parent(self) -> Parent:
        parent = Parent(item_data=ItemData(item_id=self.item_id,
                                           external_id=self.external_id,
                                           item=self.item),
                        embedding=self.embedding,
                        assembly_index=self.assembly_index)
        return parent

class InternalAssemblyData(BaseModel):
    assembly_id: int 
    parents: List[AssemblyItemInternal]
        
class ScoreResult(BaseModel):
    plugin_id: int 
    plugin_name: str 
    score: float

class InternalItem(BaseModel):
    item_data: ItemData
    valid: bool=True
    score: Optional[ScoreResult]
    embeddings: Dict[int, Embedding]
    assembly_data: Optional[InternalAssemblyData]
    query_group: Optional[int]
    update_embedding: Optional[Embedding]=None
