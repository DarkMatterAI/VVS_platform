from pydantic import BaseModel
from typing import Optional, Union, List
from datetime import datetime

class ItemBase(BaseModel):
    item: str

class ItemCreate(ItemBase):
    pass

class ItemInDB(ItemBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ItemSourceBase(BaseModel):
    external_id: Optional[str] = None
    plugin_id: int

class ItemSourceCreate(ItemSourceBase):
    pass

class ItemSourceInDB(ItemSourceBase):
    item_id: int
    created_at: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ItemResultBase(BaseModel):
    plugin_id: int
    valid: bool
    score: Optional[float] = None
    embedding: Optional[List[float]] = None

class ItemResultCreate(ItemResultBase):
    pass

class ItemResultInDB(ItemResultBase):
    item_id: int
    created_at: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class NewItem(BaseModel):
    external_id: Optional[Union[int, str]]
    item: str

class NewResult(BaseModel):
    item_id: int 
    valid: bool
    score: Optional[float] = None
    embedding: Optional[List[float]] = None

# class AssemblyParent(BaseModel):
#     item_id: int 
#     assembly_index: int 

# class NewAssembly(BaseModel):
#     external_id: Optional[Union[int, str]]
#     item: str
#     parents: List[AssemblyParent]


# class RequestData(BaseModel):
#     """Data for plugin making a request"""
#     request_id: Optional[str]
#     plugin_id: int 
#     plugin_name: str 
        
# class ItemData(BaseModel):
#     """Data for item in a request"""
#     item_id: int
#     external_id: Optional[Union[int, str]]
#     item: str 

# class AssemblyItem(ItemData):
#     assembly_index: int 

# class AssemblyRequest(BaseModel):
#     request_data: RequestData
#     parents: List[AssemblyItem]

# class AssemblyResult(BaseModel):
#     item: str 
#     external_id: Optional[Union[int, str]]
        
# class AssemblyResponse(BaseModel):
#     valid: bool
#     result: List[AssemblyResult]