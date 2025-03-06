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
    external_id: Optional[str]
    item: str

class NewResult(BaseModel):
    item_id: int 
    valid: bool
    score: Optional[float] = None
    embedding: Optional[List[float]] = None
