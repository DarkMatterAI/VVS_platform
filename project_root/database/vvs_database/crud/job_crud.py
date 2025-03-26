from typing import Optional, List, Dict, Any
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from vvs_database.models.job_models import Job, JobPlugin
from vvs_database.schemas.enums import JobStatus, JobType

async def create_job(db: AsyncSession,
                     job_type: JobType,
                     job_json: Optional[Dict[str, Any]] = None,
                     status: JobStatus = JobStatus.CREATED
                    ) -> Job:
    """Create a new job."""
    job = Job(job_type=job_type, job_json=job_json, status=status)
    db.add(job)
    await db.commit()
    return job

async def get_job(db: AsyncSession,
                  job_id: int,
                  load_plugins: bool = False
                 ) -> Optional[Job]:
    """Get a job by ID with optional loading of plugins relationship."""
    query = select(Job).filter(Job.id == job_id)
    
    if load_plugins:
        query = query.options(selectinload(Job.plugins))
    
    async with db.begin():
        result = await db.execute(query)
    
    result = result.scalar_one_or_none()
    
    if load_plugins and (result is not None):
        await db.refresh(result, ["plugins"])
        for jp in result.plugins:
            await db.refresh(jp, ["plugin"])
        await db.commit()
            
    return result

async def update_job(db: AsyncSession,
                     job_id: int,
                     status: Optional[JobStatus] = None,
                     job_json: Optional[Dict[str, Any]] = None
                    ) -> Optional[Job]:
    """Update a job's status and/or job_json."""
    update_data = {}
    if status is not None:
        update_data["status"] = status
    if job_json is not None:
        update_data["job_json"] = job_json
    
    if not update_data:
        return await get_job(db, job_id)
    
    stmt = update(Job).where(Job.id == job_id).values(**update_data).returning(Job)
    async with db.begin():
        result = await db.execute(stmt)
        job = result.scalar_one_or_none()
    
    return job

async def delete_job(db: AsyncSession,
                     job: Job
                    ) -> Job:
    """Delete a job."""
    await db.delete(job)
    await db.commit()
    return job

async def create_job_plugin(db: AsyncSession,
                            job_id: int,
                            plugin_id: int
                           ) -> JobPlugin:
    """Create a new job-plugin association."""
    job_plugin = JobPlugin(job_id=job_id, plugin_id=plugin_id)
    db.add(job_plugin)
    await db.commit()
    return job_plugin

async def bulk_create_job_plugins(db: AsyncSession,
                                  job_id: int,
                                  plugin_ids: List[int]
                                 ) -> List[JobPlugin]:
    """Bulk create multiple job-plugin associations."""
    job_plugins = [JobPlugin(job_id=job_id, plugin_id=plugin_id) for plugin_id in plugin_ids]
    db.add_all(job_plugins)
    await db.commit()
    
    # Refresh all created objects
    for job_plugin in job_plugins:
        await db.refresh(job_plugin)
    await db.commit()
    
    return job_plugins

async def get_job_plugin(db: AsyncSession,
                         job_id: int,
                         plugin_id: int
                        ) -> Optional[JobPlugin]:
    """Get a specific job-plugin association."""
    async with db.begin():
        result = await db.execute(
            select(JobPlugin).filter(
                JobPlugin.job_id == job_id,
                JobPlugin.plugin_id == plugin_id
            )
        )
    return result.scalar_one_or_none()

async def get_job_plugins(db: AsyncSession,
                          job_id: int
                         ) -> List[JobPlugin]:
    """Get all plugin associations for a job."""
    async with db.begin():
        result = await db.execute(
            select(JobPlugin).filter(JobPlugin.job_id == job_id)
        )
    return result.scalars().all()

async def delete_job_plugin(db: AsyncSession,
                            job_plugin: JobPlugin
                           ) -> JobPlugin:
    """Delete a job-plugin association."""
    await db.delete(job_plugin)
    await db.commit()
    return job_plugin