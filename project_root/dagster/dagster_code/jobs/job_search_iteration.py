from dagster import Config, job, op

from sqlalchemy import text
from typing import Optional, List 
from enum import Enum
import uuid 
import json 
import datetime 

from ..resources.postgres_resource import PostgresResourceConfig

class InputItem(Config):
    unique_item_id: int
    external_id: str 
    item: str 


class QueryTypes(str, Enum):
    STANDARD='standard'
    MAPPER='mapper'
    DECOMPOSED='decomposed'

class DataQueryConfig(Config):
    job_id: int 
    job_input_id: int 
    iteration: int 
    remaining: int 
    input_items: List[InputItem]
    mapper_id: List[int]
    data_source_ids: List[int]
    assembly_ids: List[int]
    filter_ids: List[int]
    score_ids: List[int]


def infer_query_type(config):
    if config.mapper_id:
        query_type = 'mapper'
    elif len(config.input_items) == 1 and len(config.data_source_ids) == 1:
        query_type = 'standard'
    else:
        query_type = 'decomposed'
    return query_type 

def postgres_query_get_plugins(context, conn, plugin_ids):
    context.log.info(f"Postgres query plugin records for plugins {plugin_ids}")
    query = """
    SELECT p.*, mp.input_embedding_id
    FROM plugins p
    LEFT JOIN mapper_plugins mp ON p.id = mp.id
    WHERE p.id = ANY(:ids)
    """
    query_result = conn.execute(text(query), {"ids": plugin_ids})
    return {row.id: row._asdict() for row in query_result}

def postgres_query_get_plugin_embeddings(context, conn, plugin_id):
    context.log.info(f"Postgres query linked embedding records for plugin {plugin_id}")
    query = """
    SELECT p.*, ep.vector_length, ep.distance_metric
    FROM plugins p
    INNER JOIN embedding_plugins ep ON p.id = ep.id
    LEFT JOIN plugin_embeddings pe ON p.id = pe.embedding_id
    WHERE pe.plugin_id = :id OR p.id = (
        SELECT input_embedding_id 
        FROM mapper_plugins 
        WHERE id = :id
    )
    """
    query_result = conn.execute(text(query), {"id": plugin_id})
    records = [row._asdict() for row in query_result]
    record_ids = [i['id'] for i in records]

    return records, record_ids

def pull_records_from_config(context, engine, config, query_type):
    output = {
        'mapper_record' : None,
        'data_source_records' : {},
        'filter_records' : {},
        'score_records' : {},
        'assembly_records' : {},
        'input_embedding_records' : {},
        'output_embedding_records' : {}
    }

    with engine.connect() as conn:
        plugin_ids = config.mapper_id + config.data_source_ids + config.filter_ids + config.score_ids + config.assembly_ids
        records = postgres_query_get_plugins(context, conn, plugin_ids)
        embedding_records = {}
        for record_id, record in records.items():
            context.log.info(f"{record}")
            if record['type'].lower() == 'mapper':
                output['mapper_record'] = record
            else:
                key = f"{record['type'].lower()}_records"
                output[key][record['id']] = record
            
            embedding_records_iter, embedding_ids = postgres_query_get_plugin_embeddings(context, conn, record_id)
            record['embedding_ids'] = embedding_ids
            embedding_records.update({i['id']:i for i in embedding_records_iter})
            
        if query_type == 'mapper':
            mapper_record = output['mapper_record']
            input_embedding_id = mapper_record['input_embedding_id']
            input_embedding = embedding_records.pop(input_embedding_id)
            output['input_embedding_records'][input_embedding_id] = input_embedding
        else:
            for record in output['data_source_records'].values():
                for embedding_id in record['embedding_ids']:
                    input_embedding = embedding_records.pop(embedding_id)
                    output['input_embedding_records'][embedding_id] = input_embedding
        output['output_embedding_records'].update(embedding_records)
    return output 


@op
def pull_data_records(context, config: DataQueryConfig, postgres: PostgresResourceConfig):
    context.log.info(f"Pulling down records for {config}")

    query_type = infer_query_type(config)

    output = {
        'job_id' : config.job_id,
        'job_input_id' : config.job_input_id,
        'iteration' : config.iteration,
        'remaining' : config.remaining,
        'query_type' : query_type,
        'input_items' : [i.model_dump() for i in config.input_items],
        'mapper_record' : None,
        'data_source_records' : {},
        'filter_records' : {},
        'score_records' : {},
        'assembly_records' : {},
        'input_embedding_records' : {},
        'output_embedding_records' : {}
    }

    engine = postgres.get_engine()
    records = pull_records_from_config(context, engine, config, query_type)
    output.update(records)
    return output 

def serialize_datetime(o): 
    if isinstance(o, datetime.datetime):
        return o.__str__()

@op 
def save_to_json(context, records):
    filename = f"{records['job_id']}_{records['job_input_id']}_{records['iteration']}_{uuid.uuid4()}.json"
    context.log.info(f"Saving to {filename}")
    with open(filename, 'w') as f:
        json.dump(records, f, default=serialize_datetime)

@job
def parse_search_config_job():
    # 1. pull records from backend
    records = pull_data_records()

    # 2. compute relevant embeddings of input items

    # 3. save to json 
    save_to_json(records)
