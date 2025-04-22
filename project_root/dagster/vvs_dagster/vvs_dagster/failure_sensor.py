import dagster as dg 
from vvs_dagster.resources import PostgresResource, RedisResource
from vvs_dagster.utils import get_logger
import asyncio

from vvs_database.crud import update_job_from_dagster_id
from vvs_database import schemas 


async def clear_semaphores(redis_resource: RedisResource, records: list, logging):
    logging.info(f"Clearing semaphores for {len(records)} jobs")
    redis_service = redis_resource.get_service()
    for record in records:
        await redis_service.clear_job_semaphores(job_id=record.id)
    await redis_service.close()

def update_job_wrapper(postgres_resource: PostgresResource,
                       redis_resource: RedisResource,
                       dagster_run_id: str,
                       status: schemas.JobStatus,
                       logging):
    logging.info(f"Updating jobs with dagster run id {dagster_run_id} with status {status}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = postgres_resource.get_db()

    records = loop.run_until_complete(update_job_from_dagster_id(session, dagster_run_id, status=status))
    loop.run_until_complete(session.close())
    loop.run_until_complete(clear_semaphores(redis_resource, records, logging))
    loop.close()
    return records 

@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING)
def job_failure_sensor(context: dg.RunFailureSensorContext,
                       postgres_resource: PostgresResource,
                       redis_resource: RedisResource):
    logging = get_logger(context)
    job_name = context.dagster_run.job_name
    context.log.info(f"Job '{job_name}' has failed.")
    update_job_wrapper(postgres_resource, 
                       redis_resource, 
                       context.dagster_run.run_id, 
                       schemas.JobStatus.FAILED,
                       logging)

@dg.run_status_sensor(run_status=dg.DagsterRunStatus.CANCELED,
                      default_status=dg.DefaultSensorStatus.RUNNING)
def job_canceled_sensor(context: dg.RunStatusSensorContext,
                        postgres_resource: PostgresResource,
                        redis_resource: RedisResource):
    logging = get_logger(context)
    job_name = context.dagster_run.job_name
    context.log.info(f"Job '{job_name}' was canceled.")
    update_job_wrapper(postgres_resource, 
                       redis_resource, 
                       context.dagster_run.run_id, 
                       schemas.JobStatus.CANCELLED,
                       logging)

