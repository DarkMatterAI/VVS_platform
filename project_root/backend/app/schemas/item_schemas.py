from pydantic import BaseModel
from typing import Optional, Union, List
from datetime import datetime

class Item(BaseModel):
    id: int
    item: str
    created_at: datetime

    class Config:
        orm_mode = True

class ItemSource(BaseModel):
    item_id: int
    external_id: Optional[Union[int, str]]
    source_plugin_id: int
    created_at: datetime

    class Config:
        orm_mode = True

class ItemCheckinResponse(BaseModel):
    items: List[Item]
    item_sources: List[ItemSource]

class NewItem(BaseModel):
    external_id: Optional[Union[int, str]]
    item: str

