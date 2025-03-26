from pydantic import BaseModel
from typing import Optional 
from datetime import datetime

from vvs_database.schemas.enums import JobStatus, JobType

class JobDBResponse(BaseModel):
    id: int 
    job_type: JobType
    job_json: Optional[dict]=None
    status: JobStatus
    created_at: datetime
    updated_at: Optional[datetime] = None

