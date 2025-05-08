import dagster as dg 
from sqlalchemy import select 
import asyncio 

from vvs_dagster.resources import (
    PostgresResource, 
    RabbitMQResource, 
    RedisResource,
)
from vvs_dagster.utils import get_logger, get_connection

from vvs_database import crud, schemas, models 
from vvs_database.job_runner.hc_runner import HCRunner

class HCDagsterConfig(dg.Config):
    job_id: int 

@dg.op()
async def init_hc_job(context: dg.OpExecutionContext,
                      postgres_resource: PostgresResource,
                      config: HCDagsterConfig
                      ) -> int:
    
    logging = get_logger(context)
    db_session = postgres_resource.get_db()

    logging.info(f"Initializing HC Job {config.job_id}")
    job = await crud.get_job(db_session, config.job_id, with_error=True)

    current_status = job.status
    expected_status = [schemas.JobStatus.CREATED, schemas.JobStatus.QUEUED]
    if current_status not in expected_status:
        raise Exception(f"Invalid job status {current_status}, expected one of {expected_status}")
    
    if job.job_type != schemas.JobType.HILL_CLIMB_JOB:
        raise Exception(f"Invalid job type {job.job_type}, expected {schemas.JobType.HILL_CLIMB_JOB}")
    
    job = await crud.update_job(db_session, 
                                job.id, 
                                status=schemas.JobStatus.RUNNING,
                                dagster_run_id=context.run.run_id)
    await db_session.close()
    
    return job.id 

@dg.op(out=dg.DynamicOut())
async def spawn_input_jobs(context: dg.OpExecutionContext,
                           postgres_resource: PostgresResource,
                           job_id: int):
    logging = get_logger(context)
    db_session = postgres_resource.get_db()

    logging.info(f"Fanning out input jobs")

    stmt = select(models.HCInputJob).filter(models.HCInputJob.parent_id==job_id)
    res = await db_session.execute(stmt)
    records = res.scalars().all()
    input_ids = [i.id for i in records]
    await db_session.close()

    for idx, input_id in enumerate(input_ids):
        yield dg.DynamicOutput(input_id, mapping_key=str(idx))

import time 

@dg.op(pool="hc_input")
async def input_job(context: dg.OpExecutionContext,
                    postgres_resource: PostgresResource,
                    rabbitmq_resource: RabbitMQResource,
                    redis_resource: RedisResource,
                    input_job_id: int):
    
    logging = get_logger(context)
    connections = get_connection(postgres_resource,
                                 rabbitmq_resource,
                                 redis_resource)
    db_session = connections.db_service.db
    
    logging.info(f"Starting HC Input Job {input_job_id}")
    runner = HCRunner(input_job_id)

    await runner.load_job(db_session)
    runner.load_ops(connections)

    logging.info(f"Initializing HC Input Job {input_job_id}")
    await runner.init_job(connections)
    iter_job = await runner.init_first_iteration(db_session)

    logging.info(f"Running HC Input Job {input_job_id}")

    try:
        while iter_job is not None:
            iter_job = await runner(connections)
    except Exception as e:
        await connections.redis_service.clear_job_semaphores(input_job_id)
        await db_session.close()
        await connections.close()
        raise e

    await db_session.close()
    await connections.close()

    return True 

@dg.op
async def collate_results(context: dg.OpExecutionContext,
                          input_job_results: list):
    logging = get_logger(context)
    logging.info(f"Collating job results")
    time.sleep(5)
    return True 


default_config = dg.RunConfig(
    ops={"init_hc_job": HCDagsterConfig(job_id=1)}
)

@dg.job(config=default_config)
def hc_job():
    job_id = init_hc_job()
    input_jobs = spawn_input_jobs(job_id=job_id)
    input_job_results = input_jobs.map(lambda input_job_id: input_job(input_job_id=input_job_id))
    collate = collate_results(input_job_results=input_job_results.collect())
    

@dg.sensor(job=hc_job,
           minimum_interval_seconds=5,
        #    default_status=dg.DefaultSensorStatus.RUNNING,
           )
def hc_sensor(context: dg.SensorEvaluationContext,
              postgres_resource: PostgresResource):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    last_job = int(context.cursor) if context.cursor else 0
    new_last_job = last_job

    session = postgres_resource.get_db()
    filter_params = {
        'job_type' : schemas.JobType.HILL_CLIMB_JOB,
        'status' : schemas.JobStatus.CREATED,
        'auto_execute' : True
    }

    records = loop.run_until_complete(crud.get_jobs(session, filter_params))

    context.log.info(f'Hill climb sensor: found {len(records)} jobs')

    for record in records:
        job_id = record.id
        if job_id > last_job:
            new_last_job = max(new_last_job, job_id)
            run_config = {"ops": {"init_hc_job": {"config": {"job_id": job_id}}}}
            context.log.info(f'Yielding run request for job {job_id}')
            yield dg.RunRequest(run_key=str(record.id), run_config=run_config)

            context.log.info(f'Updating status for job {job_id}')
            loop.run_until_complete(crud.update_job(session, job_id, status=schemas.JobStatus.QUEUED))

    context.update_cursor(str(new_last_job))
    loop.run_until_complete(session.close())
    loop.close()
    
