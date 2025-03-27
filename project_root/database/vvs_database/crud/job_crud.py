from typing import Optional, List, Dict, Any
from sqlalchemy import select, update, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from vvs_database.models import (
    DataSourcePlugin, 
    FilterPlugin, 
    ScorePlugin,
    MapperPlugin, 
    Job,
    JobPlugin
)

from vvs_database.crud.plugin_crud import (
    build_filters,
    get_plugin
)
from vvs_database.crud.s3_crud import check_file_exists
from vvs_database.schemas import (
    PluginClass,
    JobStatus, 
    JobType, 
    CreateQdrantUploadJob
)
from vvs_database.exceptions import NotFoundError, ValidationError

async def create_job(db: AsyncSession,
                     job_type: JobType,
                     job_json: Optional[Dict[str, Any]] = None,
                     status: JobStatus = JobStatus.CREATED,
                     status_detail: Optional[Dict[str, Any]] = None,
                    ) -> Job:
    """Create a new job."""
    job = Job(job_type=job_type, 
              job_json=job_json, 
              status=status,
              status_detail=status_detail)
    db.add(job)
    await db.commit()
    return job

async def get_job(db: AsyncSession,
                  job_id: int,
                  load_plugins: bool = False,
                  with_error: bool = False 
                 ) -> Optional[Job]:
    """Get a job by ID with optional loading of plugins relationship."""
    query = select(Job).filter(Job.id == job_id)
    
    if load_plugins:
        query = query.options(selectinload(Job.plugins))
    
    async with db.begin():
        result = await db.execute(query)
    
    result = result.scalar_one_or_none()

    if with_error and (result is None):
        raise NotFoundError(f"Plugin with ID {job_id} not found")
    
    if load_plugins and (result is not None):
        await db.refresh(result, ["plugins"])
        for jp in result.plugins:
            await db.refresh(jp, ["plugin"])
            plugin = jp.plugin 
            await db.refresh(plugin)
            if isinstance(plugin, (DataSourcePlugin, FilterPlugin, ScorePlugin, MapperPlugin)):
                await db.refresh(plugin, ["embeddings"])
        await db.commit()

    return result

async def get_jobs(db: AsyncSession, 
                   filter_params: Dict[str, Any] = None,
                   skip: int = 0, 
                   limit: int = 100,
                   ):
    """Get jobs with filtering, pagination and eager loading."""
    stmt = (
        select(Job)
        .options(
            selectinload(Job.plugins),  # Eager load related plugins
        )
    )

    if filter_params:
        filters = build_filters(Job, filter_params)
        stmt = stmt.filter(and_(*filters))

    # Order by created_at descending to get newest jobs first
    stmt = stmt.order_by(Job.created_at.desc())
    
    stmt = stmt.offset(skip).limit(limit)

    result = await db.execute(stmt)
    jobs = result.scalars().all()
    
    for job in jobs:
        await db.refresh(job)
    
    return jobs

async def update_job(db: AsyncSession,
                     job_id: int,
                     job_json: Optional[Dict[str, Any]] = None,
                     status: Optional[JobStatus] = None,
                     status_detail: Optional[Dict[str, Any]] = None
                    ) -> Optional[Job]:
    """Update a job's status and/or job_json."""
    update_data = {}
    if status is not None:
        update_data["status"] = status
    if job_json is not None:
        update_data["job_json"] = job_json
    if status_detail is not None:
        update_data["status_detail"] = status_detail
    
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

async def validate_qdrant_upload_create(db: AsyncSession, 
                                        create_data: CreateQdrantUploadJob):
    if create_data.filename is not None:
        file_exists = check_file_exists(create_data.filename)
        if not file_exists:
            raise ValidationError(f'Filename {create_data.filename} not found')
                                              
    data_record = await get_plugin(db, create_data.plugin_id, with_error=True)
    if data_record.plugin_class != PluginClass.INTERNAL_QDRANT:
        raise ValidationError(f'Plugin must be of class {PluginClass.INTERNAL_QDRANT}, ' \
                              f'found {data_record.plugin_class}')
    
    embeddings = {i.id : i for i in data_record.embeddings}
    if create_data.embedding_configs is not None:
        for embedding_config in create_data.embedding_configs:
            if embedding_config.plugin_id not in embeddings:
                raise ValidationError(f"Found config for embedding {embedding_config.plugin_id}, " \
                                      f"expected one of {embeddings.keys()}")
                
    return data_record, embeddings

async def create_qdrant_upload_job(db: AsyncSession, 
                                   create_data: CreateQdrantUploadJob, 
                                   test=False):
    data_record, embeddings = await validate_qdrant_upload_create(db, create_data)
    
    if test:
        job_type = JobType.TEST_JOB
    else:
        job_type = JobType.QDRANT_UPLOAD
        
    job_json = create_data.model_dump()
    
    job = await create_job(db, job_type, job_json)
    
    plugin_ids = [data_record.id] + list(embeddings.keys())
    
    job_plugins = await bulk_create_job_plugins(db, job.id, plugin_ids)
    
    return job, job_plugins
