from pydantic import BaseModel
from typing import List, Union, Optional

class RequestData(BaseModel):
    """Data for plugin making a request"""
    request_id: Optional[str]
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

class EmbedResponse(BaseModel):
    valid: bool 
    embedding: Optional[List[float]]

class DataSourceRequest(BaseModel):
    request_data: RequestData
    embedding: Embedding
    k: int 
        
class DataSourceResponseItem(BaseModel):
    item: str
    external_id: Optional[Union[int, str]]
    embedding: List[float]
    distance: Optional[float]
        
class DataSourceResponse(BaseModel):
    valid: bool
    result: List[DataSourceResponseItem]
        
class FilterResponse(BaseModel):
    valid: bool
        
class ScoreResponse(BaseModel):
    valid: bool
    score: float
        
class MapperRequest(BaseModel):
    request_data: RequestData
    embedding: Embedding
        
class MapperResponse(BaseModel):
    valid: bool
    embedding: List[List[float]]

class AssemblyItem(ItemData):
    assembly_index: int 

class AssemblyRequest(BaseModel):
    request_data: RequestData
    parents: List[AssemblyItem]

class AssemblyResult(BaseModel):
    item: str 
    external_id: Optional[Union[int, str]]
        
class AssemblyResponse(BaseModel):
    valid: bool
    result: List[AssemblyResult]

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

request_response_schema_mapping = {
    'embedding': {'request': ItemRequest, 'response': EmbedResponse},
    'data_source': {'request': DataSourceRequest, 'response': DataSourceResponse},
    'filter': {'request': ItemRequest, 'response': FilterResponse},
    'score': {'request': ItemRequest, 'response': ScoreResponse},
    'mapper': {'request': MapperRequest, 'response': MapperResponse},
    'assembly': {'request': AssemblyRequest, 'response': AssemblyResponse},
}



# class EmbedRequest(BaseModel):
#     request_id: str 
#     id: Union[int, str]
#     external_id: Optional[Union[int, str]]
#     item: str 

# class EmbedResponse(BaseModel):
#     embedding: List[float]

# class NamedEmbedding(BaseModel):
#     embedding_id: int
#     embedding_name: str
#     embedding: List[float]

# class DataSourceRequest(BaseModel):
#     request_id: str 
#     embedding_id: int 
#     embedding_name: str 
#     embedding: List[float]
#     k: int 

# class DataSourceResponseItem(BaseModel):
#     external_id: Optional[Union[int, str]]
#     item: str 
#     embedding: List[float]
#     distance: Optional[float]
        
# class DataSourceResponse(BaseModel):
#     valid: bool
#     result: List[DataSourceResponseItem]

# class ItemRequest(BaseModel):
#     request_id: str 
#     id: Union[int, str]
#     external_id: Optional[Union[int, str]]
#     item: str 
#     embedding: List[NamedEmbedding]
        
# class FilterResponse(BaseModel):
#     valid: bool
        
# class ScoreResponse(BaseModel):
#     valid: bool
#     score: float

# class MapperRequest(BaseModel):
#     request_id: str 
#     id: int # internal id
#     name: str
#     embedding: List[float]
        
# class MapperResponse(BaseModel):
#     valid: bool
#     embedding: List[List[float]]

# class AssemblyItem(BaseModel):
#     assembly_index: int 
#     id: Union[int, str]
#     external_id: Optional[Union[int, str]]
#     item: str 

# class AssemblyRequest(BaseModel):
#     request_id: str 
#     parents: List[AssemblyItem]

# class AssemblyResult(BaseModel):
#     item: str 
#     external_id: Optional[Union[int, str]]
        
# class AssemblyResponse(BaseModel):
#     valid: bool
#     result: List[AssemblyResult]

class RedisResult(BaseModel):
    result_id: str 

# ExecuteRequestUnion = Union[
#     ItemRequest,
#     AssemblyRequest,
#     DataSourceRequest,
#     EmbedRequest,
#     MapperRequest
# ]

# BatchExecuteRequestUnion = Union[
#     List[ItemRequest],
#     List[AssemblyRequest],
#     List[DataSourceRequest],
#     List[EmbedRequest],
#     List[MapperRequest]
# ]

# request_response_schema_mapping = {
#     'embedding': {'request': EmbedRequest, 'response': EmbedResponse},
#     'data_source': {'request': DataSourceRequest, 'response': DataSourceResponse},
#     'filter': {'request': ItemRequest, 'response': FilterResponse},
#     'score': {'request': ItemRequest, 'response': ScoreResponse},
#     'mapper': {'request': MapperRequest, 'response': MapperResponse},
#     'assembly': {'request': AssemblyRequest, 'response': AssemblyResponse},
# }

