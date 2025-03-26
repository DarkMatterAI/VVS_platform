import dagster as dg 
from dagster_aws.s3 import S3Resource

import pandas as pd
from typing import Optional

from vvs_database import settings 
from vvs_database.schemas import ExecutePlugin, ExecuteParams

class DGExecuteParams(ExecuteParams, dg.Config):
    pass 

class DGExecutePlugin(ExecutePlugin, dg.Config):
    execute_params: DGExecuteParams

class QdrantUploadConfig(dg.Config):
    plugin_id: int 
    embedding_config: Optional[DGExecutePlugin]=None
    filename: str 

@dg.op
def get_job_data(context: dg.OpExecutionContext, 
                 config: QdrantUploadConfig):
    return {'plugin_id' : config.plugin_id, 'filename' : config.filename}


@dg.op(out=dg.DynamicOut())
def load_pieces(context: dg.OpExecutionContext,
                s3_resource: S3Resource,
                job_data: dict):
    
    context.log.info(f"Getting object from S3")
    s3_client = s3_resource.get_client()
    response = s3_client.get_object(Bucket=settings.S3_BUCKET, 
                                      Key=job_data['filename'])
    context.log.info(f"Loading chunks")
    file_data = response['Body']
    chunksize=5
    for idx, chunk in enumerate(pd.read_csv(file_data, chunksize=chunksize)):
        yield dg.DynamicOutput(chunk, mapping_key=str(idx))

@dg.op
def process_chunk(context: dg.OpExecutionContext,
                  chunk: pd.DataFrame,
                  job_data: dict):
    context.log.info(chunk.head())

default_config = dg.RunConfig(
    ops={"get_job_data": QdrantUploadConfig(plugin_id=1,
                                            embedding_config=None,
                                            filename='')}
)

@dg.job(config=default_config)
def qdrant_upload_job():
    job_data = get_job_data()
    chunks = load_pieces(job_data=job_data)
    chunks.map(lambda chunk: process_chunk(chunk=chunk, job_data=job_data))



