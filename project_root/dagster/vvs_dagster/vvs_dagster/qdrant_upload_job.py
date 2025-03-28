import dagster as dg 
from dagster_aws.s3 import S3Resource
from qdrant_client import models 

from typing import Tuple 
import pandas as pd 
import uuid 

from vvs_database import crud, schemas 
from vvs_database.job_runner import QdrantUploadRunner
from vvs_database.execution.connections import Connections

from vvs_dagster.resources import (
    PostgresResource, 
    RabbitMQResource, 
    RedisResource,
    QdrantResource
)

class QdrantUploadConfig(dg.Config):
    job_id: int 

def get_logger(context: dg.OpExecutionContext):
    from vvs_database import logging 
    logging.set_logger(context.log)
    return logging

def get_connection(postgres_resource: PostgresResource,
                   rabbitmq_resource: RabbitMQResource,
                   redis_resource: RedisResource):
    db_service = postgres_resource.get_service()
    redis_service = redis_resource.get_service()
    rabbitmq_service = rabbitmq_resource.get_service()
    return Connections(db_service=db_service,
                       redis_service=redis_service,
                       rabbitmq_service=rabbitmq_service)

@dg.op(out={"runner": dg.Out(), "filename": dg.Out(), "collection_name": dg.Out()})
async def load_job_data(context: dg.OpExecutionContext,
                        postgres_resource: PostgresResource,
                        qdrant_resource: QdrantResource,
                        config: QdrantUploadConfig
                        ) -> Tuple[QdrantUploadRunner, str, str]:
    # setup 
    logging = get_logger(context)
    db_session = postgres_resource.get_db()
    qdrant_client = qdrant_resource.get_service()
    runner = QdrantUploadRunner(config.job_id)

    # load jobs
    await runner.load_job(db_session)
    await runner.update_job(db_session, schemas.JobStatus.RUNNING)

    # setup qdrant indexing threshold to 0 prior to upload 
    await qdrant_resource.set_indexing_threshold(logging, qdrant_client, runner.collection_name, 0)

    await db_session.close()
    await qdrant_client.close()

    filename = runner.job.job_json['filename']
    collection_name = runner.collection_name

    return runner, filename, collection_name

@dg.op(out=dg.DynamicOut())
def chunk_csv_dynamic(context: dg.OpExecutionContext,
                      s3_resource: S3Resource,
                      qdrant_resource: QdrantResource,
                      filename: str):
    logging = get_logger(context)
    s3_client = s3_resource.get_client()
    response = crud.get_file(filename, s3_client)
    file_data = response['Body']

    for idx, chunk in enumerate(pd.read_csv(file_data, chunksize=qdrant_resource.upload_job_chunksize)):
        yield dg.DynamicOutput(chunk, mapping_key=str(idx))

@dg.op(pool="plugin_execution")
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
                        collection_name: str):
    # setup 
    logging = get_logger(context)
    qdrant_client = qdrant_resource.get_service()

    points, failed = qdrant_resource.qdrant_records_to_points(records)
    _ = qdrant_resource.upload_points(logging, qdrant_client, collection_name, points )
    await qdrant_client.close()

    return failed 

@dg.op
async def collect_qdrant_results(context: dg.OpExecutionContext,
                                 postgres_resource: PostgresResource,
                                 qdrant_resource: QdrantResource,
                                 runner: QdrantUploadRunner,
                                 failed: list[list[dict]]):
    
    # setup
    logging = get_logger(context)
    db_session = postgres_resource.get_db()
    qdrant_client = qdrant_resource.get_service()

    # log failures
    failed = [item for sublist in failed for item in sublist]
    await runner.save_failed(db_session, failed)

    # build index
    index_log = await qdrant_resource.index_sleep(logging, 
                                                  qdrant_client,
                                                  runner.collection_name)

    index_log['num_failed'] = len(failed)

    if runner.job.job_json['save_snapshot']:
        logging.info(f'Saving snapshot')
        response = await qdrant_client.create_snapshot(runner.collection_name)

    await runner.update_job(db_session, 
                            status=schemas.JobStatus.COMPLETE,
                            status_detail=index_log)
    
    await db_session.close()
    await qdrant_client.close()

default_config = dg.RunConfig(
    ops={"load_job_data": QdrantUploadConfig(job_id=1)}
)

@dg.job(config=default_config)
def qdrant_upload_job():
    runner, filename, collection_name = load_job_data()
    upload_chunks = chunk_csv_dynamic(filename=filename)
    records = upload_chunks.map(lambda df: qdrant_upload_embed(df=df, runner=runner))
    failed = records.map(lambda rec: qdrant_upload(records=rec, collection_name=collection_name))
    collect_qdrant_results(runner=runner, failed=failed.collect())


