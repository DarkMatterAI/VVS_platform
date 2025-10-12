from pydantic import BaseModel, model_validator
from typing import Optional, List, Union, Generic, TypeVar
from datetime import datetime

from vvs_database.schemas.enums import JobStatus, JobType
from vvs_database.schemas.internal_schemas import ExecutePlugin, ExecutePluginCreate

T = TypeVar("T")

class JobDBResponse(BaseModel):
    id: int 
    job_type: JobType
    # job_json: Optional[dict]=None
    status: JobStatus
    status_detail: Optional[dict]=None
    dagster_run_id: Optional[str]=None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class QdrantUploadJobDBResponse(JobDBResponse):
    num_uploaded: Optional[int]=None
    num_failed: Optional[int]=None
    index_time: Optional[float]=None
    index_timeout: Optional[bool]=None
    index_error: Optional[bool]=None

JobDBResponseUnion = Union[JobDBResponse,
                           QdrantUploadJobDBResponse]

class UserItem(BaseModel):
    external_id: Optional[str]
    item: str 

class QdrantUploadJobParams(BaseModel):
    plugin_id: int 
    filename: Optional[str]=None
    items: Optional[List[UserItem]]=None
    save_snapshot: bool=False 
    build_index: bool=True 
        
    @model_validator(mode='after')
    def check_consistency(self):
        if (self.filename is None) and (self.items is None):
            raise ValueError("Expected one of filename, items, found none")
        if (self.filename is not None) and (self.items is not None):
            raise ValueError("Expected one of filename, items, found both")
        if (self.items is not None) and (len(self.items)==0):
            raise ValueError("items list must have at least one item")
        return self

class QdrantUploadBase(QdrantUploadJobParams, Generic[T]):
    embedding_configs: Optional[List[T]]=None 

CreateQdrantUploadJob = QdrantUploadBase[ExecutePluginCreate]
QdrantUploadInternal = QdrantUploadBase[ExecutePlugin]

