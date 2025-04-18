from pydantic import BaseModel, model_validator
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

class PluginOverrideParams(BaseModel):
    timeout: Optional[int] = None
    max_concurrency: Optional[int] = None
    max_retries: Optional[int] = None
    batch_size: Optional[int] = None
    endpoint_url: Optional[str] = None
    group_key: Optional[str] = None

class ExecuteDataParams(BaseModel):
    k: int 
    assembly_index: int=0

class ExecutePluginParams(BaseModel):
    plugin_id: int
    execute_params: Optional[ExecuteParams]=None
    override_params: Optional[PluginOverrideParams]=None
    runtime_args: Optional[dict]=None

    @model_validator(mode='after')
    def check_consistency(self):
        if self.execute_params is None:
            self.execute_params = ExecuteParams()
        if self.override_params is None:
            self.override_params = PluginOverrideParams()
        return self

class ExecutePluginCreate(ExecutePluginParams):
    # create - plugin params
    pass 

class ExecuteDataSourceCreate(ExecutePluginParams):
    data_source_params: ExecuteDataParams

class ExecutePlugin(ExecutePluginCreate, PluginRecord):
    # internal - plugin params, plugin
    pass 

class ExecuteDataSource(ExecuteDataSourceCreate, PluginRecord):
    data_source_params: ExecuteDataParams

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
