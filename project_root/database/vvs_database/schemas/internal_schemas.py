from pydantic import BaseModel, model_validator
from typing import List, Union, Optional, Dict
import numpy as np 
from collections import defaultdict

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

class AssembledEmbedding(BaseModel):
    embedding: Embedding
    assembly_index: int
        
class QueryEmbedding(BaseModel):
    query_group: int 
    embedding: Optional[Embedding]
    assembled_embeddings: Optional[List[AssembledEmbedding]]

class Query(BaseModel):
    queries: List[QueryEmbedding]
        
    def to_embeddings(self) -> List[Embedding]:
        return [i.embedding for i in self.queries]
    
    def to_embedding_dict(self) -> Dict[int, List[Embedding]]:
        embedding_dict = defaultdict(list)
        for query in self.queries:
            for embedding in query.assembled_embeddings:
                embedding_dict[embedding.assembly_index].append(embedding.embedding)
        return embedding_dict
    
    def update_from_mapper(self, mapper_result: Dict[int, List[Embedding]]):
        for query in self.queries:
            if query.assembled_embeddings is None:
                query.assembled_embeddings = []
                
            for assembly_index, results in mapper_result.items():
                result = results[query.query_group]
                result = AssembledEmbedding(embedding=result, 
                                            assembly_index=assembly_index)
                query.assembled_embeddings.append(result)
            query.assembled_embeddings = sorted(query.assembled_embeddings,
                                                key=lambda x: x.assembly_index)

class GradientEmbedding(Embedding):
    gradient: Optional[List[float]]
    learning_rates: List[float]
    assembly_index: int 
    
    def get_embeddings(self) -> List[AssembledEmbedding]:
        embeddings = [self.embedding]
        if self.gradient is not None:
            embedding = np.array(self.embedding)
            gradient = np.array(self.gradient)
            for lr in self.learning_rates:
                embeddings.append((embedding - lr * gradient).tolist())
                
        output = []
        for embedding in embeddings:
            embedding = Embedding(plugin_id=self.plugin_id,
                                  plugin_name=self.plugin_name,
                                  embedding=embedding)
            embedding = AssembledEmbedding(embedding=embedding,
                                           assembly_index=self.assembly_index)
            output.append(embedding)
        return output 
