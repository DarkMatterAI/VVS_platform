# from pydantic import BaseModel
# from typing import Optional, Union, List
# from datetime import datetime

# class Item(BaseModel):
#     id: int
#     item: str
#     created_at: datetime

#     class Config:
#         orm_mode = True

# class ItemSource(BaseModel):
#     item_id: int
#     external_id: Optional[Union[int, str]]
#     plugin_id: int
#     created_at: datetime

#     class Config:
#         orm_mode = True

# class ItemCheckinResponse(BaseModel):
#     items: List[Item]
#     item_sources: List[ItemSource]

# class NewItem(BaseModel):
#     external_id: Optional[Union[int, str]]
#     item: str

# class NewScore(BaseModel):
#     item_id: int 
#     score: float 

# class ItemScore(BaseModel):
#     item_id: int 
#     plugin_id: int 
#     score: float 
#     created_at: datetime 

#     class Config:
#         orm_mode = True

# class AssemblyComponent(BaseModel):
#     item_id: int 
#     assembly_index: int 

# class NewAssembly(BaseModel):
#     item: str 
#     external_id: Optional[Union[int, str]]
#     parents: List[AssemblyComponent]
