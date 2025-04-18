from sqlalchemy.ext.asyncio import AsyncSession

from app.utils import handle_db_exception

from vvs_database import crud

async def get_job(db: AsyncSession, 
                  job_id: int,
                  with_error: bool=True):
    try:
        job = await crud.get_job(db, job_id, with_error=with_error)
    except Exception as e:
        handle_db_exception(e)
    return job

async def delete_job(db: AsyncSession, 
                     job_id: int):
    try:
        job = await crud.get_job(db, job_id, with_error=True)
        job = await crud.delete_job(db, job)
    except Exception as e:
        handle_db_exception(e)
    return job

async def kill_job(db: AsyncSession,
                   job_id: int):
    try:
        job = await crud.kill_job(db, job_id, with_error=True)
        # refresh for weird greenlet spawn bullshit 
        job = await crud.get_job(db, job_id, with_error=True)
    except Exception as e:
        handle_db_exception(e)
    return job
