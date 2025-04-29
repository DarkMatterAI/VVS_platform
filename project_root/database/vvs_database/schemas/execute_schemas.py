from pydantic import BaseModel, ConfigDict
from typing import List, Union, Optional, Dict

class RequestData(BaseModel):
    """Data for plugin making a request"""
    request_id: Optional[str] # request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}
    plugin_id: int 
    plugin_name: str 

class ItemData(BaseModel):
    """Data for item in a request"""
    item_id: int
    external_id: Optional[str]
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
    runtime_args: Optional[Dict]=None

    def generate_key(self, plugin_id: int):
        return f"plugin:{plugin_id}:item:{self.item_data.item_id}"
    
    @staticmethod
    def strip_key(key):
        return key.split(':item:')[1]

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
    runtime_args: Optional[Dict]=None

    def generate_key(self, plugin_id: int):
        request_id = self.request_data.request_id.split('.')[-1]
        return f"plugin:{plugin_id}:datasource:{request_id}"
    
    @staticmethod
    def strip_key(key):
        return key.split(':datasource:')[1]
        
class DataSourceResponseItem(BaseModel):
    model_config = ConfigDict(extra='allow')
    item: str
    # external_id: Optional[Union[int, str]]
    external_id: Optional[str]
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
    runtime_args: Optional[Dict]=None

    def generate_key(self, plugin_id: int):
        request_id = self.request_data.request_id.split('.')[-1]
        return f"plugin:{plugin_id}:mapper:{request_id}"
    
    @staticmethod
    def strip_key(key):
        return key.split(':mapper:')[1]
        
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
    runtime_args: Optional[Dict]=None

    def generate_key(self, plugin_id: int):
        sorted_parents = sorted(self.parents, key=lambda x: x.assembly_index)
        parent_ids = [str(i.item_id) for i in sorted_parents]
        key = f"plugin:{plugin_id}:assembly:{'_'.join(parent_ids)}"
        return key 

    @staticmethod
    def strip_key(key):
        return key.split(':assembly:')[1]
    
    def generate_component_key(self, plugin_id: int):
        sorted_parents = sorted(self.parents, key=lambda x: x.assembly_index)
        parent_ids = [i.item_id for i in sorted_parents]
        component_key = f"{plugin_id}_{'_'.join(map(str, parent_ids))}"
        return component_key

class AssemblyResult(BaseModel):
    model_config = ConfigDict(extra='allow')
    item: str 
    external_id: Optional[str]
        
class AssemblyResponse(BaseModel):
    valid: bool
    result: Optional[List[AssemblyResult]]

    @classmethod
    def failure_response(cls):
        return cls(valid=False, result=None)
    
class ExecuteParams(BaseModel):
    cache: bool                   = False 
    db_lookup: bool               = False
    db_persist: bool              = False
    use_semaphore: bool           = True
    max_semaphore_attempts: int   = 20
    queue_polling_interval: float = 0.2
    backoff_factor: float         = 2.0
    log_execute_keys: bool        = False


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

