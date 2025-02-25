from pydantic import BaseModel
from typing import Optional, List
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

class ItemScoreBase(BaseModel):
    plugin_id: int
    score: float

class ItemScoreCreate(ItemScoreBase):
    pass

class ItemScoreInDB(ItemScoreBase):
    item_id: int
    created_at: datetime

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }