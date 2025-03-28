import dagster as dg 
from dagster_aws.s3 import S3Resource
from qdrant_client import models 

from typing import Tuple 
import pandas as pd 
import uuid 
import time 
import asyncio 

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

async def set_indexing_threshold(logging, qdrant_client, collection_name, threshold):
    logging.info(f'Setting qdrant indexing threshold to {threshold}')
    await qdrant_client.update_collection(collection_name,
                                          optimizers_config=models.OptimizersConfigDiff(
                                              indexing_threshold=0))

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

    # setup qdrant
    await set_indexing_threshold(logging, qdrant_client, runner.collection_name, 0)

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

def qdrant_records_to_points(records: list[dict]):
    points = []
    failed = []

    for record in records:
        payload = record["item_data"]
        if not record["valid"]:
            failed.append(payload)
            continue 

        point = models.PointStruct(id=str(uuid.uuid4()),
                                   payload=payload,
                                   vector={f"embedding_{plugin_id}" : embedding 
                                           for plugin_id, embedding in record['embeddings'].items()})
        points.append(point)
    return points, failed 



@dg.op(pool="qdrant_upload")
async def qdrant_upload(context: dg.OpExecutionContext,
                        qdrant_resource: QdrantResource,
                        records: list[dict],
                        collection_name: str):
    # setup 
    logging = get_logger(context)
    qdrant_client = qdrant_resource.get_service()

    points, failed = qdrant_records_to_points(records)

    _ = qdrant_client.upload_points(collection_name=collection_name,
                                    points=points,
                                    parallel=qdrant_resource.upload_processes,
                                    max_retries=qdrant_resource.upload_max_retries,
                                    batch_size=qdrant_resource.upload_batch_size)
    await qdrant_client.close()

    return failed 

async def qdrant_index_sleep(logging,
                             qdrant_client, 
                             index_start, 
                             collection_name, 
                             upload_ping, 
                             index_timeout):
    logging.info('Waiting on index build')
    await asyncio.sleep(1.0) # wait for indexing to start 

    index_log = {'index_timeout' : False,
                 'index_error' : False}
    while True:
        elapsed = time.time() - index_start 
        if elapsed > index_timeout:
            index_log['index_timeout'] = True 
            return index_log 
        
        collection_data = await qdrant_client.get_collection(collection_name)
        status = collection_data.status
        if status == 'green':
            return index_log  

        elif status == 'yellow':
            logging.info(f'Index sleep, {elapsed} elapsed')
            await asyncio.sleep(upload_ping)

        else:
            index_log['index_error'] = True
            return index_log 

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

    # start indexing
    index_start = time.time()
    await set_indexing_threshold(logging, qdrant_client, runner.collection_name, 1)

    # log failures
    failed = [item for sublist in failed for item in sublist]
    await runner.save_failed(db_session, failed)

    # wait for indexing
    index_log = await qdrant_index_sleep(logging, 
                                         qdrant_client, 
                                         index_start,
                                         runner.collection_name, 
                                         qdrant_resource.upload_ping,
                                         qdrant_resource.indexing_timeout)
    index_log['num_failed'] = len(failed)
    index_log['index_time'] = time.time() - index_start 

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


