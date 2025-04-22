import dagster as dg 
from sqlalchemy import select 

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

@dg.op
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
    while iter_job is not None:
        iter_job = await runner(connections)

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
    
    # runner, user_data, job_params = load_job_data()
    # upload_chunks = chunk_csv_dynamic(user_data=user_data)
    # records = upload_chunks.map(lambda df: qdrant_upload_embed(df=df, runner=runner))
    # upload_output = records.map(lambda rec: qdrant_upload(records=rec, job_params=job_params))
    # upload_summary = save_failed_results(runner=runner, upload_output=upload_output.collect())
    # index_log = build_qdrant_index(upload_summary=upload_summary, job_params=job_params)
    # update_job_complete(runner=runner, index_log=index_log)



# @dg.op(out=dg.DynamicOut())
# def chunk_csv_dynamic(context: dg.OpExecutionContext,
#                       s3_resource: S3Resource,
#                       qdrant_resource: QdrantResource,
#                       user_data: dict):
#     logging = get_logger(context)

#     filename = user_data['filename']
#     items = user_data['items']
#     chunksize = qdrant_resource.upload_job_chunksize

#     if filename is not None:
#         s3_client = s3_resource.get_client()
#         response = crud.get_file(filename, s3_client)
#         file_data = response['Body']
#         chunk_iterator = pd.read_csv(file_data, chunksize=chunksize)
#     else:
#         df = pd.DataFrame(items)
#         chunk_iterator = [df[i:i+chunksize] for i in range(0, df.shape[0], chunksize)]

#     for idx, chunk in enumerate(chunk_iterator):
#         yield dg.DynamicOutput(chunk, mapping_key=str(idx))





# import dagster as dg 
# from dagster_aws.s3 import S3Resource

# from typing import Tuple 
# import pandas as pd 
# import asyncio

# from vvs_database import crud, schemas 
# from vvs_database.job_runner import QdrantUploadRunner
# from vvs_database.execution.connections import Connections

# from vvs_dagster.resources import (
#     PostgresResource, 
#     RabbitMQResource, 
#     RedisResource,
#     QdrantResource
# )


# @dg.op(out=dg.DynamicOut())
# def chunk_csv_dynamic(context: dg.OpExecutionContext,
#                       s3_resource: S3Resource,
#                       qdrant_resource: QdrantResource,
#                       user_data: dict):
#     logging = get_logger(context)

#     filename = user_data['filename']
#     items = user_data['items']
#     chunksize = qdrant_resource.upload_job_chunksize

#     if filename is not None:
#         s3_client = s3_resource.get_client()
#         response = crud.get_file(filename, s3_client)
#         file_data = response['Body']
#         chunk_iterator = pd.read_csv(file_data, chunksize=chunksize)
#     else:
#         df = pd.DataFrame(items)
#         chunk_iterator = [df[i:i+chunksize] for i in range(0, df.shape[0], chunksize)]

#     for idx, chunk in enumerate(chunk_iterator):
#         yield dg.DynamicOutput(chunk, mapping_key=str(idx))

# @dg.op(pool="qdrant_embed")
# async def qdrant_upload_embed(context: dg.OpExecutionContext,
#                               postgres_resource: PostgresResource,
#                               rabbitmq_resource: RabbitMQResource,
#                               redis_resource: RedisResource,
#                               df: pd.DataFrame,
#                               runner: QdrantUploadRunner
#                               ) -> list[dict]:
#     # setup
#     logging = get_logger(context)
#     connections = get_connection(postgres_resource,
#                                  rabbitmq_resource,
#                                  redis_resource)
    
#     records = df.to_dict(orient='records')
#     records = await runner.execute_item_ops(records, connections)

#     await connections.close()
#     await connections.db_service.db.close()

#     return records 

# @dg.op(pool="qdrant_upload")
# async def qdrant_upload(context: dg.OpExecutionContext,
#                         qdrant_resource: QdrantResource,
#                         records: list[dict],
#                         job_params: dict
#                         ) -> dict:
#     # setup 
#     logging = get_logger(context)
#     qdrant_client = qdrant_resource.get_service()
#     collection_name = job_params['collection_name']

#     points, failed = qdrant_resource.qdrant_records_to_points(records)
#     _ = qdrant_resource.upload_points(logging, qdrant_client, collection_name, points )
#     await qdrant_client.close()

#     upload_output = {
#         "failed": failed,
#         "num_uploaded": len(points)
#     }

#     return upload_output 

# @dg.op
# async def save_failed_results(context: dg.OpExecutionContext,
#                               postgres_resource: PostgresResource,
#                               runner: QdrantUploadRunner,
#                               upload_output: list[dict]
#                               ) -> dict:
#     # setup
#     logging = get_logger(context)
#     db_session = postgres_resource.get_db()
#     failed = []
#     num_uploaded = 0
#     for output in upload_output:
#         failed += output['failed']
#         num_uploaded += output['num_uploaded']

#     num_failed = len(failed)
#     output = {
#         "num_failed": num_failed,
#         "num_uploaded": num_uploaded
#     }

#     # log failures
#     await runner.save_failed(db_session, failed)
#     await db_session.close()

#     return output

# @dg.op(pool="qdrant_index_build")
# async def build_qdrant_index(context: dg.OpExecutionContext,
#                              qdrant_resource: QdrantResource,
#                              upload_summary: dict,
#                              job_params: dict
#                              ) -> dict:
#     # setup
#     logging = get_logger(context)
#     qdrant_client = qdrant_resource.get_service()
#     collection_name = job_params['collection_name']
#     save_snapshot = job_params['save_snapshot']

#     # build index
#     index_log = await qdrant_resource.index_sleep(logging, 
#                                                   qdrant_client,
#                                                   collection_name)

#     index_log.update(upload_summary)

#     collection_info = await qdrant_resource.get_collection_info(logging,
#                                                                 qdrant_client,
#                                                                 collection_name)
#     index_log['collection_info'] = collection_info

#     if save_snapshot:
#         logging.info(f'Saving snapshot')
#         response = await qdrant_client.create_snapshot(collection_name)

#     await qdrant_client.close()

#     return index_log 

# @dg.op
# async def update_job_complete(context: dg.OpExecutionContext,
#                               postgres_resource: PostgresResource,
#                               runner: QdrantUploadRunner,
#                               index_log: dict):
#     # setup
#     logging = get_logger(context)
#     db_session = postgres_resource.get_db()

#     collection_info = index_log.pop('collection_info')

#     await runner.update_job(db_session, 
#                             status=schemas.JobStatus.COMPLETE,
#                             num_uploaded=index_log['num_uploaded'],
#                             num_failed=index_log['num_failed'],
#                             index_time=index_log['index_time'],
#                             index_timeout=index_log['index_timeout'],
#                             index_error=index_log['index_error'])
    
#     logging.info("Updating collection info")
#     plugin = await crud.get_plugin(db_session, runner.data_source_id)
#     config = dict(plugin.config)
#     config['collection_info'] = collection_info
#     setattr(plugin, 'config', config)
#     await db_session.commit()
    
#     await db_session.close()





