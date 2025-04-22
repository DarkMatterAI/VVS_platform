import dagster as dg 
from dagster_aws.s3 import S3Resource

from typing import Tuple 
import pandas as pd 
import asyncio

from vvs_database import crud, schemas 
from vvs_database.job_runner import QdrantUploadRunner
# from vvs_database.execution.connections import Connections

from vvs_dagster.resources import (
    PostgresResource, 
    RabbitMQResource, 
    RedisResource,
    QdrantResource
)

from vvs_dagster.utils import get_logger, get_connection

class QdrantUploadConfig(dg.Config):
    job_id: int 

# def get_connection(postgres_resource: PostgresResource,
#                    rabbitmq_resource: RabbitMQResource,
#                    redis_resource: RedisResource):
#     db_service = postgres_resource.get_service()
#     redis_service = redis_resource.get_service()
#     rabbitmq_service = rabbitmq_resource.get_service()
#     return Connections(db_service=db_service,
#                        redis_service=redis_service,
#                        rabbitmq_service=rabbitmq_service)

@dg.op(out={"runner": dg.Out(), "user_data": dg.Out(), "job_params": dg.Out()})
async def load_job_data(context: dg.OpExecutionContext,
                        postgres_resource: PostgresResource,
                        qdrant_resource: QdrantResource,
                        config: QdrantUploadConfig
                        ) -> Tuple[QdrantUploadRunner, dict, dict]:
    # setup 
    logging = get_logger(context)
    db_session = postgres_resource.get_db()
    qdrant_client = qdrant_resource.get_service()
    runner = QdrantUploadRunner(config.job_id)

    # load jobs
    await runner.load_job(db_session)

    current_status = runner.job.status
    expected_status = [schemas.JobStatus.CREATED, schemas.JobStatus.QUEUED]
    if current_status not in expected_status:
        raise Exception(f"Invalid job status {current_status}, expected one of {expected_status}")

    await runner.update_job(db_session, 
                            status=schemas.JobStatus.RUNNING,
                            dagster_run_id=context.run.run_id)

    # setup qdrant indexing threshold to 0 prior to upload 
    await qdrant_resource.set_indexing_threshold(logging, qdrant_client, runner.collection_name, 0)

    await db_session.close()
    await qdrant_client.close()

    user_data = {
        'filename' : runner.job.job_json['filename'],
        'items' : runner.job.job_json['items']
    }

    job_params = {
        'collection_name' : runner.collection_name,
        'save_snapshot' : runner.job.job_json['save_snapshot']
    }

    return runner, user_data, job_params

@dg.op(out=dg.DynamicOut())
def chunk_csv_dynamic(context: dg.OpExecutionContext,
                      s3_resource: S3Resource,
                      qdrant_resource: QdrantResource,
                      user_data: dict):
    logging = get_logger(context)

    filename = user_data['filename']
    items = user_data['items']
    chunksize = qdrant_resource.upload_job_chunksize

    if filename is not None:
        s3_client = s3_resource.get_client()
        response = crud.get_file(filename, s3_client)
        file_data = response['Body']
        chunk_iterator = pd.read_csv(file_data, chunksize=chunksize)
    else:
        df = pd.DataFrame(items)
        chunk_iterator = [df[i:i+chunksize] for i in range(0, df.shape[0], chunksize)]

    for idx, chunk in enumerate(chunk_iterator):
        yield dg.DynamicOutput(chunk, mapping_key=str(idx))

@dg.op(pool="qdrant_embed")
async def qdrant_upload_embed(context: dg.OpExecutionContext,
                              postgres_resource: PostgresResource,
                              rabbitmq_resource: RabbitMQResource,
                              redis_resource: RedisResource,
                              df: pd.DataFrame,
                              runner: QdrantUploadRunner
                              ) -> list[dict]:
    # setup
    logging = get_logger(context)
    connections = get_connection(postgres_resource,
                                 rabbitmq_resource,
                                 redis_resource)
    
    records = df.to_dict(orient='records')
    records = await runner.execute_item_ops(records, connections)

    await connections.close()
    await connections.db_service.db.close()

    return records 

@dg.op(pool="qdrant_upload")
async def qdrant_upload(context: dg.OpExecutionContext,
                        qdrant_resource: QdrantResource,
                        records: list[dict],
                        job_params: dict
                        ) -> dict:
    # setup 
    logging = get_logger(context)
    qdrant_client = qdrant_resource.get_service()
    collection_name = job_params['collection_name']

    points, failed = qdrant_resource.qdrant_records_to_points(records)
    _ = qdrant_resource.upload_points(logging, qdrant_client, collection_name, points )
    await qdrant_client.close()

    upload_output = {
        "failed": failed,
        "num_uploaded": len(points)
    }

    return upload_output 

@dg.op
async def save_failed_results(context: dg.OpExecutionContext,
                              postgres_resource: PostgresResource,
                              runner: QdrantUploadRunner,
                              upload_output: list[dict]
                              ) -> dict:
    # setup
    logging = get_logger(context)
    db_session = postgres_resource.get_db()
    failed = []
    num_uploaded = 0
    for output in upload_output:
        failed += output['failed']
        num_uploaded += output['num_uploaded']

    num_failed = len(failed)
    output = {
        "num_failed": num_failed,
        "num_uploaded": num_uploaded
    }

    # log failures
    await runner.save_failed(db_session, failed)
    await db_session.close()

    return output

@dg.op(pool="qdrant_index_build")
async def build_qdrant_index(context: dg.OpExecutionContext,
                             qdrant_resource: QdrantResource,
                             upload_summary: dict,
                             job_params: dict
                             ) -> dict:
    # setup
    logging = get_logger(context)
    qdrant_client = qdrant_resource.get_service()
    collection_name = job_params['collection_name']
    save_snapshot = job_params['save_snapshot']

    # build index
    index_log = await qdrant_resource.index_sleep(logging, 
                                                  qdrant_client,
                                                  collection_name)

    index_log.update(upload_summary)

    collection_info = await qdrant_resource.get_collection_info(logging,
                                                                qdrant_client,
                                                                collection_name)
    index_log['collection_info'] = collection_info

    if save_snapshot:
        logging.info(f'Saving snapshot')
        response = await qdrant_client.create_snapshot(collection_name)

    await qdrant_client.close()

    return index_log 

@dg.op
async def update_job_complete(context: dg.OpExecutionContext,
                              postgres_resource: PostgresResource,
                              runner: QdrantUploadRunner,
                              index_log: dict):
    # setup
    logging = get_logger(context)
    db_session = postgres_resource.get_db()

    collection_info = index_log.pop('collection_info')

    await runner.update_job(db_session, 
                            status=schemas.JobStatus.COMPLETE,
                            num_uploaded=index_log['num_uploaded'],
                            num_failed=index_log['num_failed'],
                            index_time=index_log['index_time'],
                            index_timeout=index_log['index_timeout'],
                            index_error=index_log['index_error'])
    
    logging.info("Updating collection info")
    plugin = await crud.get_plugin(db_session, runner.data_source_id)
    config = dict(plugin.config)
    config['collection_info'] = collection_info
    setattr(plugin, 'config', config)
    await db_session.commit()
    
    await db_session.close()


default_config = dg.RunConfig(
    ops={"load_job_data": QdrantUploadConfig(job_id=1)}
)


@dg.job(config=default_config)
def qdrant_upload_job():
    runner, user_data, job_params = load_job_data()
    upload_chunks = chunk_csv_dynamic(user_data=user_data)
    records = upload_chunks.map(lambda df: qdrant_upload_embed(df=df, runner=runner))
    upload_output = records.map(lambda rec: qdrant_upload(records=rec, job_params=job_params))
    upload_summary = save_failed_results(runner=runner, upload_output=upload_output.collect())
    index_log = build_qdrant_index(upload_summary=upload_summary, job_params=job_params)
    update_job_complete(runner=runner, index_log=index_log)


@dg.sensor(job=qdrant_upload_job,
           minimum_interval_seconds=60,
        #    default_status=dg.DefaultSensorStatus.RUNNING,
           )
def qdrant_upload_sensor(context: dg.SensorEvaluationContext,
                         postgres_resource: PostgresResource):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    last_job = int(context.cursor) if context.cursor else 0
    new_last_job = last_job

    session = postgres_resource.get_db()
    filter_params = {
        'job_type' : schemas.JobType.QDRANT_UPLOAD,
        'status' : schemas.JobStatus.CREATED,
        'auto_execute' : True
    }

    records = loop.run_until_complete(crud.get_jobs(session, filter_params))

    context.log.info(f'Qdrant upload sensor: found {len(records)} jobs')

    for record in records:
        job_id = record.id
        if job_id > last_job:
            new_last_job = max(new_last_job, job_id)
            run_config = {"ops": {"load_job_data": {"config": {"job_id": job_id}}}}
            context.log.info(f'Yielding run request for job {job_id}')
            yield dg.RunRequest(run_key=str(record.id), run_config=run_config)

            context.log.info(f'Updating status for job {job_id}')
            loop.run_until_complete(crud.update_job(session, job_id, status=schemas.JobStatus.QUEUED))

    context.update_cursor(str(new_last_job))
    loop.run_until_complete(session.close())
    loop.close()
    


