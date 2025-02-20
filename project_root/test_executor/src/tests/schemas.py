from pydantic import BaseModel
from typing import List, Union, Optional

class EmbedRequest(BaseModel):
    request_id: str 
    id: Union[int, str] # internal unique item id
    external_id: Optional[Union[int, str]] # external id
    item: str 

class EmbedResponse(BaseModel):
    embedding: List[float]

class NamedEmbedding(BaseModel):
    embedding_id: int # internal id
    embedding_name: str
    embedding: List[float]

class DataSourceRequest(BaseModel):
    request_id: str 
    embedding_id: int 
    embedding_name: str 
    embedding: List[float]
    k: int 

class DataSourceResponseItem(BaseModel):
    external_id: Optional[Union[int, str]] # external id from data source
    item: str 
    embedding: List[float]
    distance: Optional[float]
        
class DataSourceResponse(BaseModel):
    valid: bool
    result: List[DataSourceResponseItem]

class ItemRequest(BaseModel):
    request_id: str 
    id: Union[int, str]
    external_id: Optional[Union[int, str]]
    item: str 
    embedding: List[NamedEmbedding]
        
class FilterResponse(BaseModel):
    valid: bool
        
class ScoreResponse(BaseModel):
    valid: bool
    score: float

class MapperRequest(BaseModel):
    request_id: str 
    id: int # internal id
    name: str
    embedding: List[float]
        
class MapperResponse(BaseModel):
    valid: bool
    embedding: List[List[float]]

class AssemblyItem(BaseModel):
    assembly_index: int 
    id: Union[int, str]
    external_id: Optional[Union[int, str]]
    item: str 

class AssemblyRequest(BaseModel):
    request_id: str 
    parents: List[AssemblyItem]

class AssemblyResult(BaseModel):
    item: str 
    external_id: Optional[Union[int, str]]
        
class AssemblyResponse(BaseModel):
    valid: bool
    result: List[AssemblyResult]

schema_mapping = {
    'embedding' : {'request' : EmbedRequest, 'response' : EmbedResponse},
    'data_source' : {'request' : DataSourceRequest, 'response' : DataSourceResponse},
    'filter' : {'request' : ItemRequest, 'response' : FilterResponse},
    'score' : {'request' : ItemRequest, 'response' : ScoreResponse},
    'mapper' : {'request' : MapperRequest, 'response' : MapperResponse},
    'assembly' : {'request' : AssemblyRequest, 'response' : AssemblyResponse},
}


