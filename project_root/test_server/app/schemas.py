from pydantic import BaseModel
from typing import List, Union, Optional

class EmbedRequest(BaseModel):
    request_id: str 
    id: Union[int, str] # internal unique item id
    external_id: Union[int, str] # external id
    item: str 

class EmbedResponse(BaseModel):
    embedding: List[float]

class NamedEmbedding(BaseModel):
    id: int # internal id
    name: str
    embedding: List[float]
    gradient: Optional[List[float]]=None

class DataSourceRequest(BaseModel):
    request_id: str 
    embedding: List[NamedEmbedding]
    k: int 

class DataSourceResponseItem(BaseModel):
    external_id: Union[int, str] # external id from data source
    item: str 
    embedding: List[List[float]]
    distance: Optional[List[float]]
        
class DataSourceResponse(BaseModel):
    valid: bool
    result: List[DataSourceResponseItem]

class ItemRequest(BaseModel):
    request_id: str 
    id: Union[int, str]
    external_id: Union[int, str]
    item: str 
    embedding: List[NamedEmbedding]
        
class FilterResponse(BaseModel):
    valid: bool
        
class ScoreResponse(BaseModel):
    valid: bool
    score: float

class MapperRequest(BaseModel):
    request_id: str 
    embedding: NamedEmbedding
        
class MapperResponse(BaseModel):
    valid: bool
    embedding: List[List[float]]

class AssemblyItem(BaseModel):
    id: Union[int, str]
    external_id: Union[int, str]
    item: str 

class AssemblyRequest(BaseModel):
    request_id: str 
    parents: List[AssemblyItem]
        
class AssemblyResponse(BaseModel):
    valid: bool
    item: str 
    external_id: Optional[Union[int, str]]



# class UpdateRequest(BaseModel):
#     request_id: str 
#     query_embedding: List[NamedEmbedding]
