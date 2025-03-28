from pydantic import BaseModel, ConfigDict
from typing import List, Union, Optional, Dict

from vvs_database.schemas.execute_schemas import ExecuteParams
from vvs_database.schemas.execute_schemas import (
    ItemData,
    Embedding,
    AssemblyItem
)

class ExecutePlugin(BaseModel):
    model_config = ConfigDict(extra='allow')
    plugin_id: int
    execute_params: ExecuteParams
    runtime_args: Optional[dict]=None

class ExecuteDataSource(ExecutePlugin):
    k: int 
    assembly_index: int

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
