import dagster as dg 
from dagster_aws.s3 import S3Resource
from qdrant_client import models 

import uuid 
import pandas as pd 
from typing import Optional

from vvs_database import crud, schemas 
from vvs_database.execution.connections import Connections
from vvs_database.execution.plugins import PluginExecutorFactory

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
    
async def load_job_plugins(logger, 
                           db_session, 
                           job_id: int,
                           status_update: Optional[schemas.JobStatus]=None):
    logger.info(f"Getting data for job {job_id}")
    if status_update is not None:
        _ = await crud.update_job(db_session, job_id, status=status_update)

    job = await crud.get_job(db_session, job_id, load_plugins=True)

    job_data = {
        'job_id' : job.id,
        'job_type' : job.job_type,
        'job_json' : job.job_json,
        'plugins' : {}
    }

    for jp in job.plugins:
        plugin = jp.plugin
        job_data['plugins'][plugin.id] = plugin

    return job_data 

def get_connection(postgres_resource: PostgresResource,
                   rabbitmq_resource: RabbitMQResource,
                   redis_resource: RedisResource):
    db_service = postgres_resource.get_service()
    redis_service = redis_resource.get_service()
    rabbitmq_service = rabbitmq_resource.get_service()
    return Connections(db_service=db_service,
                       redis_service=redis_service,
                       rabbitmq_service=rabbitmq_service)

def load_qdrant_upload_execution_configs(job_data: dict):
    job_json = schemas.CreateQdrantUploadJob(**job_data['job_json'])
    user_embedding_configs = {}
    if job_json.embedding_configs is not None:
        user_embedding_configs = {i.plugin_id:i for i in job_json.embedding_configs}

    job_data['execute_configs'] = []
    for plugin_id, plugin in job_data['plugins'].items():
        if plugin.type != 'embedding':
            continue 

        if plugin_id in user_embedding_configs:
            plugin_config = user_embedding_configs[plugin_id]
        else:
            plugin_config = schemas.ExecutePlugin(plugin_id=plugin_id,
                                                  execute_params=schemas.ExecuteParams(),
                                                  runtime_args=None)
        # disable for qdrant job
        plugin_config.execute_params.cache = False
        plugin_config.execute_params.db_persist = False
        plugin_config.execute_params.db_lookup = False 
        plugin_config.plugin = plugin 

        job_data['execute_configs'].append(plugin_config)
    return job_data 

@dg.op
async def load_job_data(context: dg.OpExecutionContext,
                        postgres_resource: PostgresResource,
                        qdrant_resource: QdrantResource,
                        config: QdrantUploadConfig):

    logging = get_logger(context)

    db_session = postgres_resource.get_db()
    job_data = await load_job_plugins(logging, db_session, 
                                      config.job_id, schemas.JobStatus.RUNNING)
    job_data = load_qdrant_upload_execution_configs(job_data)

    logging.info('Setting qdrant indexing threshold to zero')
    qdrant_client = qdrant_resource.get_service()
    collection_name = f"data_source_{job_data['job_json']['plugin_id']}"
    await qdrant_client.update_collection(collection_name,
                                          optimizers_config=models.OptimizersConfigDiff(
                                              indexing_threshold=0))

    logging.info(f"Loaded job data {job_data}")
    await db_session.close()
    await qdrant_client.close()

    return job_data 

@dg.op(out=dg.DynamicOut())
def chunk_csv_dynamic(context: dg.OpExecutionContext,
                      s3_resource: S3Resource,
                      job_data: dict):
    logging = get_logger(context)
    UPLOAD_CHUNKSIZE = 20 # TODO: find better way of setting value 
    s3_client = s3_resource.get_client()
    filename = job_data['job_json']['filename']
    response = crud.get_file(filename, s3_client)
    file_data = response['Body']

    for idx, chunk in enumerate(pd.read_csv(file_data, chunksize=UPLOAD_CHUNKSIZE)):
        yield dg.DynamicOutput(chunk, mapping_key=str(idx))

def qdrant_upload_df_to_requests(records: list[dict]):
    requests = [schemas.ItemRequest(request_data=schemas.RequestData(request_id=None,
                                                                     plugin_id=-1,
                                                                     plugin_name=''),
                                    item_data=schemas.ItemDataEmbed(item_id=-1, 
                                                                    external_id=record['external_id'],
                                                                    item=record['item']),
                                      runtime_args=None)
                for record in records] 
    return requests 

@dg.op
async def process_chunk(context: dg.OpExecutionContext,
                        postgres_resource: PostgresResource,
                        rabbitmq_resource: RabbitMQResource,
                        redis_resource: RedisResource,
                        chunk: pd.DataFrame,
                        job_data: dict):
    logging = get_logger(context)
    logging.info('Getting connections')
    connections = get_connection(postgres_resource,
                                 rabbitmq_resource,
                                 redis_resource)
    connections.db_service.job_id = job_data['job_id']
    logging.info('Building requests')

    records = chunk.to_dict(orient='records')
    requests = qdrant_upload_df_to_requests(records)
    results = {}

    for plugin_config in job_data['execute_configs']:
        logging.info(f"Exeucting plugin {plugin_config.plugin.id}")
        executor = PluginExecutorFactory.create_executor(plugin_config.plugin,
                                                         connections,
                                                         plugin_config.execute_params)
        res = await executor.execute(requests, log_id=f"Job {job_data['job_id']}")
        responses, checkin_response, valid_execution = res
        results[plugin_config.plugin.id] = responses

    for i, record in enumerate(records):
        record['results'] = {}
        record['valid'] = True 
        for plugin_config in job_data['execute_configs']:
            plugin_id = plugin_config.plugin.id
            result = results[plugin_id][i]
            record['valid'] = record['valid'] and result.valid 
            record['results'][plugin_id] = result.embedding
    
    await connections.close()
    await connections.db_service.db.close()

    return records 

@dg.op
async def qdrant_upload(context: dg.OpExecutionContext,
                        qdrant_resource: QdrantResource,
                        records: list[dict],
                        job_data: dict):
    logging = get_logger(context)
    qdrant_client = qdrant_resource.get_service()

    points = []
    failed = []

    for i, record in enumerate(records):
        payload = {'item' : record['item'], 'external_id' : str(record['external_id'])}
        if not record['valid']:
            failed.append(payload)
            continue 

        point = models.PointStruct(id=str(uuid.uuid4()),
                                   payload=payload,
                                   vector={f"embedding_{plugin_id}":embedding 
                                           for plugin_id, embedding in record['results'].items()})
        points.append(point)

    QDRANT_UPLOAD_BATCH_SIZE = 8 # TODO: find better way of setting value 
    QDRANT_UPLOAD_PARALLEL = 1 # TODO: find better way of setting value 
    QDRANT_UPLOAD_RETRIES = 3 # TODO: find better way of setting value 

    # await qdrant_client.upload_points(collection_name=f"data_source_{job_data['job_json']['plugin_id']}",
    #                                   points=points,
    #                                   parallel=QDRANT_UPLOAD_PARALLEL,
    #                                   max_retries=QDRANT_UPLOAD_RETRIES,
    #                                   batch_size=QDRANT_UPLOAD_BATCH_SIZE)

    await qdrant_client.close()

    return failed 

@dg.op
async def collect_qdrant_results(context: dg.OpExecutionContext,
                                 qdrant_resource: QdrantResource,
                                 job_data: dict,
                                 failed: list[list[dict]]):
    
    logging = get_logger(context)

    logging.info('Setting qdrant indexing threshold')
    qdrant_client = qdrant_resource.get_service()
    collection_name = f"data_source_{job_data['job_json']['plugin_id']}"
    await qdrant_client.update_collection(collection_name,
                                          optimizers_config=models.OptimizersConfigDiff(
                                              indexing_threshold=1))
    
    await qdrant_client.close()

    # save failed results somewhere
    # set job to complete
    # wait on index build 

default_config = dg.RunConfig(
    ops={"load_job_data": QdrantUploadConfig(job_id=1)}
)

@dg.job(config=default_config)
def qdrant_upload_job():
    job_data = load_job_data()
    upload_chunks = chunk_csv_dynamic(job_data=job_data)
    failed = upload_chunks.map(lambda chunk: process_chunk(chunk=chunk, job_data=job_data)
                         ).map(lambda records: qdrant_upload(records=records, job_data=job_data))
    
    collect_qdrant_results(job_data=job_data, failed=failed.collect())


