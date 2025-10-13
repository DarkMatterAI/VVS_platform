from datetime import datetime, timezone 
from typing import Optional, List, Dict, Any
from sqlalchemy import select, update, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert as pg_insert

from vvs_database.crud.assembly_crud import prune_orphan_assembly_products

from vvs_database.models import (
    DataSourcePlugin, 
    FilterPlugin, 
    ScorePlugin,
    MapperPlugin, 
    Job,
    JobPlugin,
    QdrantUploadFailed,
    Assembly
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
    CreateQdrantUploadJob,
    TERMINAL_STATUSES
)
from vvs_database.utils import job_type_map, make_post_request
from vvs_database.exceptions import NotFoundError, ValidationError
from vvs_database.settings import settings 

async def cleanup_unreferenced_jobs(db: AsyncSession) -> int:
    """Delete items that aren't referenced in other tables."""
    return await Job.cleanup_unreferenced(db)

async def create_job(db: AsyncSession,
                     job_type: JobType,
                     job_json: Optional[Dict[str, Any]] = None,
                     status: JobStatus = JobStatus.CREATED,
                     status_detail: Optional[Dict[str, Any]] = None,
                     auto_execute: bool = False,
                     dagster_run_id: Optional[str] = None,
                     extra_args: Optional[dict] = None
                    ) -> Job:
    """Create a new job."""
    job_data = {
        "job_type": job_type,
        "job_json": job_json,
        "status": status,
        "status_detail": status_detail,
        "auto_execute": auto_execute,
        "dagster_run_id": dagster_run_id
    }
    if extra_args is not None:
        job_data.update(extra_args)

    job_data_model = job_type_map[job_type]['data_model']
    job = job_data_model(**job_data)
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
    
    result = await db.execute(query)
    
    result = result.scalar_one_or_none()

    if with_error and (result is None):
        raise NotFoundError(f"Plugin with ID {job_id} not found")
    
    if result is not None:
        await db.refresh(result)
        if load_plugins:
            await db.refresh(result, ["plugins"])
            for jp in result.plugins:
                await db.refresh(jp, ["plugin"])
                plugin = jp.plugin 
                await db.refresh(plugin)
                if isinstance(plugin, (DataSourcePlugin, FilterPlugin, ScorePlugin, MapperPlugin)):
                    await db.refresh(plugin, ["embeddings"])

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

async def update_helper(job: Job,
                        update_data: dict
                       ) -> Job:
    # update directly on existing record without commit 
    if 'status' in update_data:
        status = update_data['status']
        if status in TERMINAL_STATUSES:
            update_data["completed_at"] = datetime.now(timezone.utc)
        elif (status == JobStatus.RUNNING) and (job.started_at is None):
            update_data["started_at"] = datetime.now(timezone.utc)

    for key, value in update_data.items():
        setattr(job, key, value)

    job.updated_at = datetime.now(timezone.utc)
    return job

async def _update_job(db: AsyncSession,
                      job_id: int,
                      update_data: dict
                      ) -> Optional[Job]:
    job = await get_job(db, job_id)

    if 'status' in update_data:
        status = update_data['status']
        if status in TERMINAL_STATUSES:
            update_data["completed_at"] = datetime.now(timezone.utc)
        elif (status == JobStatus.RUNNING) and (job.started_at is None):
            update_data["started_at"] = datetime.now(timezone.utc)

    for key, value in update_data.items():
        setattr(job, key, value)

    job.updated_at = datetime.now(timezone.utc)

    return job 

async def update_job(db: AsyncSession,
                     job_id: int,
                     job_json: Optional[Dict[str, Any]] = None,
                     status: Optional[JobStatus] = None,
                     status_detail: Optional[Dict[str, Any]] = None,
                     auto_execute: Optional[bool] = None,
                     dagster_run_id: Optional[str] = None,
                    ) -> Optional[Job]:
    """Update a job's status and/or job_json."""
    update_data = {
        "status": status,
        "job_json": job_json,
        "status_detail": status_detail,
        "auto_execute": auto_execute,
        "dagster_run_id": dagster_run_id,
    }

    update_data = {k:v for k,v in update_data.items() if (v is not None)}
    
    if not update_data:
        return await get_job(db, job_id)
    
    job = await _update_job(db, job_id, update_data)
    await db.commit()
    
    return job

async def update_job_from_dagster_id(db: AsyncSession,
                                     dagster_run_id: str,
                                     status: Optional[JobStatus]=None,
                                     status_detail: Optional[Dict[str, Any]] = None):
    jobs = await get_jobs(db, filter_params={'dagster_run_id' : dagster_run_id})
    result = []
    for job in jobs:
        job = await update_job(db, job.id, status=status, status_detail=status_detail)
        result.append(job)
    return result 

# async def delete_job(db: AsyncSession,
#                      job: Job
#                     ) -> Job:
#     """Delete a job."""
#     await db.delete(job)
#     await db.commit()
#     return job

async def post_job_delete_cleanup(db: AsyncSession) -> dict:
    """
    Sweep artifacts left after job deletion:
      - delete assemblies unreferenced by hc_results or with invalid component counts
      - delete orphan item sources/results for former assembly products
      - delete items that are no longer referenced anywhere
    """
    deleted_assemblies = await Assembly.cleanup_unreferenced(db)
    item_stats = await prune_orphan_assembly_products(db)
    return {"assemblies_deleted": deleted_assemblies, **item_stats}

async def delete_job(
    db: AsyncSession,
    job: Job,
    # job_id: int,
    *,
    run_cleanup: bool = True,
    async_cleanup: bool = False,   # if True, return immediately
) -> int:
    """
    Quickly delete a job and all FK-cascaded rows. Optionally trigger
    post-delete cleanup (assemblies → items).
    Returns the job id.
    """
    # Optional: serialize on job id so multiple deleters don't race
    # await db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": job_id})

    job_id = job.id 

    res = await db.execute(
        delete(Job).where(Job.id == job_id).returning(Job.id)
    )
    deleted = res.scalar_one_or_none()
    await db.commit()

    if deleted is None:
        raise NotFoundError(f"Job {job_id} not found")

    if run_cleanup:
        if async_cleanup:
            # TODO: enqueue a background task (Dagster job / Celery) to run the sweeps
            pass
        else:
            # run cleanup 
            await post_job_delete_cleanup(db)

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

async def bulk_create_job_plugins(
    db: AsyncSession,
    job_id: int,
    plugin_ids: list[int],
) -> list[JobPlugin]:
    """
    Create (job_id, plugin_id) rows in **vvs_job_plugins**.

    - duplicates in *plugin_ids* are ignored  
    - only **one round-trip** and **one commit**
    """
    if not plugin_ids:
        return []

    # ------------------------------------------------------------------
    # bulk‑insert with “ignore duplicates”
    # ------------------------------------------------------------------
    stmt = (
        pg_insert(JobPlugin)
        .values([{"job_id": job_id, "plugin_id": pid} for pid in set(plugin_ids)])
        .on_conflict_do_nothing(index_elements=["job_id", "plugin_id"])
        .returning(JobPlugin.job_id, JobPlugin.plugin_id)
    )
    await db.execute(stmt)
    await db.flush()

    rows = (
        await db.execute(
            select(JobPlugin)
            .where(JobPlugin.job_id == job_id, JobPlugin.plugin_id.in_(plugin_ids))
            .order_by(JobPlugin.plugin_id)
        )
    ).scalars().all()
    await db.commit()

    return rows

async def get_job_plugin(db: AsyncSession,
                         job_id: int,
                         plugin_id: int
                        ) -> Optional[JobPlugin]:
    """Get a specific job-plugin association."""
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

async def kill_dagster_job(dagster_id: str,
                           dagster_url: Optional[str]=None):
    if dagster_url is None:
        dagster_url = f"http://dagster_webserver:{settings.DAGSTER_WEBSERVER_PORT}/dagster/graphql"

    # Define the GraphQL mutation for terminating a run
    TERMINATE_RUN_MUTATION = """
    mutation TerminateRun($runId: String!) {
    terminateRun(runId: $runId) {
        __typename
        ... on TerminateRunSuccess {
        run {
            runId
        }
        }
        ... on TerminateRunFailure {
        message
        }
        ... on RunNotFoundError {
        runId
        }
        ... on PythonError {
        message
        stack
        }
    }
    }
    """

    payload = {
        "query": TERMINATE_RUN_MUTATION,
        "variables": {"runId": dagster_id},
    }

    response = await make_post_request(payload, dagster_url, timeout=10, retries=1)
    return response 

async def kill_job(db: AsyncSession,
                   job_id: int,
                   with_error: bool=False,
                   dagster_url: Optional[str]=None
                   ) -> JobPlugin:
    job = await get_job(db, job_id, with_error=with_error)
    current_status = job.status 

    if current_status not in TERMINAL_STATUSES:
        new_status = JobStatus.CANCELLED
        dagster_id = job.dagster_run_id

        if dagster_id is not None:
            await kill_dagster_job(dagster_id, dagster_url=dagster_url)

        job = await update_job(db, job_id=job_id, status=new_status)

    return job 

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
                                   auto_execute: bool=False):
    data_record, embeddings = await validate_qdrant_upload_create(db, create_data)
    job_type = JobType.QDRANT_UPLOAD
    
    if create_data.embedding_configs is not None:
        for embedding_config in create_data.embedding_configs:
            if embedding_config.execute_params is not None:
                embedding_config.execute_params.db_lookup = False
                embedding_config.execute_params.db_persist = False
                embedding_config.execute_params.cache = False

    job_json = create_data.model_dump()
    
    job = await create_job(db, 
                           job_type=job_type, 
                           job_json=job_json,
                           auto_execute=auto_execute)
    
    plugin_ids = [data_record.id] + list(embeddings.keys())
    
    job_plugins = await bulk_create_job_plugins(db, job.id, plugin_ids)
    
    return job, job_plugins

async def create_qdrant_upload_failures(
    db: AsyncSession,
    job_id: int,
    records: list[dict],
    *,
    return_records: bool = False,
) -> list[QdrantUploadFailed] | None:

    failures = [
        QdrantUploadFailed(
            job_id=job_id,
            item=r["item"],
            external_id=r["external_id"],
        )
        for r in records
    ]
    db.add_all(failures)

    # ── 1. make PKs & defaults available inside this tx ────────────
    await db.flush()

    if return_records:
        # refresh only if caller wants the ORM objects fully populated
        for obj in failures:
            await db.refresh(obj) 

    # ── 2. durably persist everything ──────────────────────────────
    await db.commit()

    return failures if return_records else None

