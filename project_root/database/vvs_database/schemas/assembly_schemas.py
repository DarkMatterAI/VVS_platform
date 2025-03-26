from pydantic import BaseModel
from typing import List, Optional, Union 
from datetime import datetime

class AssemblyComponentBase(BaseModel):
    assembly_index: int
    component_id: int

class AssemblyComponentCreate(AssemblyComponentBase):
    pass

class AssemblyComponentInDB(AssemblyComponentBase):
    assembly_id: int

    class Config:
        from_attributes = True

class AssemblyBase(BaseModel):
    plugin_id: int
    product_id: int
    assembly_key: str

class AssemblyCreate(BaseModel):
    plugin_id: int
    product_id: int
    components: List[AssemblyComponentCreate]

class AssemblyInDB(AssemblyBase):
    assembly_id: int
    created_at: datetime
    components: List[AssemblyComponentInDB]

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class AssemblyComponent(BaseModel):
    item_id: int 
    assembly_index: int 

class NewAssembly(BaseModel):
    external_id: Optional[str]
    item: str
    components: List[AssemblyComponent]
