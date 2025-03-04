from pydantic import BaseModel
from typing import List, Union, Optional

class RequestData(BaseModel):
    """Data for plugin making a request"""
    request_id: Optional[str] # request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}
    plugin_id: int 
    plugin_name: str 
        
class ItemData(BaseModel):
    """Data for item in a request"""
    item_id: int
    external_id: Optional[Union[int, str]]
    item: str 
        
class Embedding(BaseModel):
    """Data for embedding in a request"""
    plugin_id: int 
    plugin_name: str 
    embedding: List[float]

class ItemDataEmbed(ItemData):
    embedding: Optional[List[Embedding]]=None 
        
class ItemRequest(BaseModel):
    request_data: RequestData
    item_data: ItemDataEmbed  

    def generate_key(self, plugin_id: int):
        return f"plugin:{plugin_id}:item:{self.item_data.item_id}"

class EmbedResponse(BaseModel):
    valid: bool 
    embedding: Optional[List[float]]

    @classmethod
    def failure_response(cls):
        return cls(valid=False, embedding=None)

class DataSourceRequest(BaseModel):
    request_data: RequestData
    embedding: Embedding
    k: int 

    def generate_key(self, plugin_id: int):
        request_id = self.request_data.request_id.split('.')[-1]
        return f"plugin:{plugin_id}:datasource:{request_id}"
        
class DataSourceResponseItem(BaseModel):
    item: str
    external_id: Optional[Union[int, str]]
    embedding: List[float]
    distance: Optional[float]
        
class DataSourceResponse(BaseModel):
    valid: bool
    result: Optional[List[DataSourceResponseItem]]

    @classmethod
    def failure_response(cls):
        return cls(valid=False, result=None)
        
class FilterResponse(BaseModel):
    valid: bool

    @classmethod
    def failure_response(cls):
        return cls(valid=False)
        
class ScoreResponse(BaseModel):
    valid: bool
    score: Optional[float]

    @classmethod
    def failure_response(cls):
        return cls(valid=False, score=None)
        
class MapperRequest(BaseModel):
    request_data: RequestData
    embedding: Embedding

    def generate_key(self, plugin_id: int):
        request_id = self.request_data.request_id.split('.')[-1]
        return f"plugin:{plugin_id}:mapper:{request_id}"
        
class MapperResponse(BaseModel):
    valid: bool
    embedding: Optional[List[List[float]]]

    @classmethod
    def failure_response(cls):
        return cls(valid=False, embedding=None)

class AssemblyItem(ItemData):
    assembly_index: int 

class AssemblyRequest(BaseModel):
    request_data: RequestData
    parents: List[AssemblyItem]

    def generate_key(self, plugin_id: int):
        sorted_parents = sorted(self.parents, key=lambda x: x.assembly_index)
        parent_ids = [i.item_id for i in sorted_parents]
        key = f"plugin:{plugin_id}:assembly:{'_'.join(parent_ids)}"
        return key 
    
    def generate_component_key(self, plugin_id: int):
        sorted_parents = sorted(self.parents, key=lambda x: x.assembly_index)
        parent_ids = [i.item_id for i in sorted_parents]
        component_key = f"{plugin_id}_{'_'.join(map(str, parent_ids))}"
        return component_key

class AssemblyResult(BaseModel):
    item: str 
    external_id: Optional[Union[int, str]]
        
class AssemblyResponse(BaseModel):
    valid: bool
    result: Optional[List[AssemblyResult]]

    @classmethod
    def failure_response(cls):
        return cls(valid=False, result=None)

class RedisResult(BaseModel):
    result_id: str 

ExecuteRequestUnion = Union[
    ItemRequest,
    AssemblyRequest,
    DataSourceRequest,
    MapperRequest
]

BatchExecuteRequestUnion = Union[
    List[ItemRequest],
    List[AssemblyRequest],
    List[DataSourceRequest],
    List[MapperRequest]
]

ItemResponseUnion = Union[
    EmbedResponse,
    FilterResponse,
    ScoreResponse
]

ExecuteResponseUnion = Union[
    EmbedResponse,
    DataSourceResponse,
    FilterResponse,
    ScoreResponse,
    MapperResponse,
    AssemblyResponse
]

