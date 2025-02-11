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
    external_id: Union[int, str] # external id from data source
    item: str 
    embedding: List[float]
    distance: Optional[float]
        
class DataSourceResponse(BaseModel):
    valid: bool
    result: List[DataSourceResponseItem]

class MapperRequest(BaseModel):
    request_id: str 
    id: int # internal id
    name: str
    embedding: List[float]
        
class MapperResponse(BaseModel):
    valid: bool
    embedding: List[List[float]]

