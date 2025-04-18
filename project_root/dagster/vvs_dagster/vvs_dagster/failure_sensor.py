import dagster as dg 
from vvs_dagster.resources import PostgresResource
import asyncio

from vvs_database.crud import update_job_from_dagster_id
from vvs_database import schemas 

def update_job_wrapper(postgres_resource: PostgresResource,
                       dagster_run_id: str,
                       status: schemas.JobStatus):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = postgres_resource.get_db()

    records = loop.run_until_complete(update_job_from_dagster_id(session, dagster_run_id, status=status))
    loop.run_until_complete(session.close())
    loop.close()
    return records 


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING)
def job_failure_sensor(context: dg.RunFailureSensorContext,
                       postgres_resource: PostgresResource):
    job_name = context.dagster_run.job_name
    context.log.info(f"Job '{job_name}' has failed.")
    update_job_wrapper(postgres_resource, context.dagster_run.run_id, schemas.JobStatus.FAILED)

@dg.run_status_sensor(run_status=dg.DagsterRunStatus.CANCELED,
                      default_status=dg.DefaultSensorStatus.RUNNING)
def job_canceled_sensor(context: dg.RunStatusSensorContext,
                        postgres_resource: PostgresResource):
    job_name = context.dagster_run.job_name
    context.log.info(f"Job '{job_name}' was canceled.")
    update_job_wrapper(postgres_resource, context.dagster_run.run_id, schemas.JobStatus.CANCELLED)

