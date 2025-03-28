from pydantic import BaseModel, model_validator
from typing import Optional, List 
from datetime import datetime

from vvs_database.schemas.enums import JobStatus, JobType
from vvs_database.schemas.internal_schemas import ExecutePlugin

class JobDBResponse(BaseModel):
    id: int 
    job_type: JobType
    job_json: Optional[dict]=None
    status: JobStatus
    created_at: datetime
    updated_at: Optional[datetime] = None

class UserItem(BaseModel):
    external_id: Optional[str]
    item: str 

class CreateQdrantUploadJob(BaseModel):
    plugin_id: int 
    embedding_configs: Optional[List[ExecutePlugin]]=None 
    filename: Optional[str]=None
    items: Optional[List[UserItem]]=None
    save_snapshot: bool=False 
        
    @model_validator(mode='after')
    def check_consistency(self):
        if (self.filename is None) and (self.items is None):
            raise ValueError("Expected one of filename, items, found none")
        if (self.filename is not None) and (self.items is not None):
            raise ValueError("Expected one of filename, items, found both")
        if (self.items is not None) and (len(self.items)==0):
            raise ValueError("items list must have at least one item")
        return self

