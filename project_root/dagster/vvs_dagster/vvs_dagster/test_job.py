import dagster as dg 
import time 

from vvs_dagster.resources import PostgresResource
from vvs_dagster.utils import get_logger

from vvs_database import schemas 
from vvs_database.job_runner.base_runner import JobRunner

class TestJobConfig(dg.Config):
    job_id: int 
    sleep_time: int
    iterations: int 
    error_iteration: int 

@dg.op
async def test_job_op(context: dg.OpExecutionContext,
                      postgres_resource: PostgresResource,
                      config: TestJobConfig):
    # setup 
    logging = get_logger(context)
    db_session = postgres_resource.get_db()
    runner = JobRunner(config.job_id)

    # load jobs
    await runner.load_job(db_session)

    current_status = runner.job.status
    expected_status = [schemas.JobStatus.CREATED, schemas.JobStatus.QUEUED]
    if current_status not in expected_status:
        raise Exception(f"Invalid job status {current_status}, expected one of {expected_status}")

    await runner.update_job(db_session, 
                            status=schemas.JobStatus.RUNNING,
                            dagster_run_id=context.run.run_id)
    
    logging.info(f"{runner.log_id}: Starting test job")
    for i in range(config.iterations):
        logging.info(f"{runner.log_id}: Test job iteration {i}")
        time.sleep(config.sleep_time)
        if i == config.error_iteration:
            logging.info(f"{runner.log_id}: Throwing error intentionally")
            1/0

    await runner.update_job(db_session, status=schemas.JobStatus.COMPLETE)

    logging.info(f"{runner.job_id}: Test Job Complete")

    return True 
    

default_config = dg.RunConfig(
    ops={"test_job_op": TestJobConfig(job_id=1, 
                                      sleep_time=1, 
                                      iterations=1,
                                      error_iteration=-1)}
)

@dg.job(config=default_config)
def test_job():
    res = test_job_op()

