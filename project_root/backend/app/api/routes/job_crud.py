from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app import schemas
from app import crud 
from app.core.database import get_db 

router = APIRouter()

@router.get("/{job_id}", response_model=schemas.JobDBResponse)
async def read_job(job_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.get_job(db, job_id=job_id)
    return response 

@router.get("/", response_model=List[schemas.JobDBResponse])
async def scroll_jobs(job_type: Optional[schemas.JobType]=None,
                      status: Optional[schemas.JobStatus]=None,
                      skip: int = 0,
                      limit: int = 100,
                      db: AsyncSession = Depends(get_db)):
    
    filter_params = {
        'job_type': job_type,
        'status': status,
    }
    filter_params = {k: v for k, v in filter_params.items() if v is not None}
    
    jobs = await crud.get_jobs(db, filter_params=filter_params, 
                              skip=skip, limit=limit)
    return jobs
    
@router.delete("/{job_id}", response_model=schemas.JobDBResponse)
async def delete_job(job_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.delete_job(db=db, job_id=job_id)
    return response

