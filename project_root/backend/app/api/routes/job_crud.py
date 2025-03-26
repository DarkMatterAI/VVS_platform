from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app import crud 
from app.core.database import get_db 

router = APIRouter()

@router.get("/{job_id}", response_model=schemas.JobDBResponse)
async def read_job(job_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.get_job(db, job_id=job_id)
    return response 