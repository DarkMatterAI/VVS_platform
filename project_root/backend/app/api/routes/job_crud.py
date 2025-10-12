from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from redis.asyncio import Redis

from app import schemas
from app import crud 
from app.core.database import get_db, get_redis_client

router = APIRouter()

@router.get("/job_cleanup")
async def job_cleanup(db: AsyncSession = Depends(get_db)):
    n_removed = await crud.cleanup_unreferenced_jobs(db)
    response = {'success' : True, 'removed' : n_removed}
    return response

@router.delete("/kill/{job_id}", response_model=schemas.JobDBResponseUnion)
async def kill_job(job_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.kill_job(db, job_id=job_id)
    return response 

@router.get("/{job_id}", response_model=schemas.JobDBResponseUnion)
async def read_job(job_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.get_job(db, job_id=job_id)
    return response 

@router.get("/", response_model=List[schemas.JobDBResponseUnion])
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
    
@router.delete("/{job_id}", response_model=schemas.JobDBResponseUnion)
async def delete_job(job_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.delete_job(db=db, job_id=job_id)
    return response

@router.delete("/clear_semaphores/job/{job_id:int}")
async def clear_job_semaphores_endpoint(
    job_id: int,
    redis_client: Redis = Depends(get_redis_client),
):
    removed = await crud.clear_job_semaphores(job_id=job_id, redis_client=redis_client)
    return {"success": True, "job_id": job_id, "identifiers_removed": removed}
