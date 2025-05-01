import os 
import httpx 

from app import crud
from app import schemas
from app.utils import fastapi_post_request

from vvs_database import logging 


TRITON_BASE_URL = f"http://triton_plugin:{os.environ.get('TRITON_HTTP_PORT', '')}/v2/models"

async def get_triton_model_sizes():
    payload = {
        "inputs": [
            {
                "name": "INPUT_1",
                "shape": [1, 1],
                "datatype": "BOOL",
                "data": [True]
            }
        ]
    }
    url = f"{TRITON_BASE_URL}/get_model_sizes/infer"
    response = await fastapi_post_request(data=payload, url=url, timeout=3, retries=20, retry_sleep=2)
    return {i["name"]:i["data"] for i in response['outputs']}


def triton_embedding_config(size):
    config = {
        "name": f"Triton Embedding size {size}",
        "type": "embedding",
        "plugin_class": "internal_triton",
        "execution_type" : "api",
        "group_key": "triton_plugin_embedding",
        "timeout": 600,
        "max_concurrency": os.environ.get('TRITON_REQUEST_CONCURRENCY', 24),
        "batch_size": int(os.environ.get('TRITON_EMBED_BATCH_SIZE', 1024))//2,
        "max_retries": 3,
        "endpoint_url": f"{TRITON_BASE_URL}/EMBED/infer",
        "vector_length": size,
        "distance_metric": 'Cosine',
        "config": {"request_keys": [f"compress_{size}"]}
    }
    return config 

def triton_mapper_config(input_size, output_size):
    config = {
        "name": f"Triton Mapper {input_size}->{output_size}",
        "type": "mapper",
        "plugin_class": "internal_triton",
        "execution_type" : "api",
        "group_key": "triton_plugin_mapper",
        "timeout": 600,
        "max_concurrency": os.environ.get('TRITON_REQUEST_CONCURRENCY', 24),
        "batch_size": int(os.environ.get('TRITON_MAPPER_BATCH_SIZE', 1024))//2,
        "max_retries": 3,
        "input_embedding_id": None,
        "output_order": [],
        "endpoint_url": f"{TRITON_BASE_URL}/DECOMPOSE/infer",
        "config": {"request_keys": [f"input_size_{input_size}", f"output_size_{output_size}"]}
    }
    return config 


async def init_triton_embeddings(db, embedding_sizes):
    current_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_triton',
                                                                'type' : 'embedding'})
    current_record_names = [i.name for i in current_records]
    for size in embedding_sizes:
        create_config = triton_embedding_config(size)
        if create_config["name"] in current_record_names:
            continue 

        logging.info(f"Creating triton embedding record {create_config['name']}")
        record = schemas.EmbeddingPluginCreate(**create_config)
        try:
            response = await crud.create_plugin(db=db, plugin=record)
            logging.info(f"Successfully created triton embedding record {response.id}")
        except:
            logging.warning(f"Creating triton record failed")


async def get_mapper_embeddings(db, input_size, output_size):
    logging.info(f"Looking for embedding records for size {input_size} -> {output_size}")
    input_records = await crud.get_plugins(db, 
                                           filter_params={'type' : 'embedding',
                                                          'name' : f"%Triton Embedding size {input_size}%"})
    output_records = await crud.get_plugins(db, 
                                            filter_params={'type' : 'embedding',
                                                           'name' : f"%Triton Embedding size {output_size}%"})
    if (not input_records) or (not output_records):
        logging.warning(f"Failed to find embedding records for size {input_size} -> {output_size} - aborting")
        return False, None, None 
    
    input_embedding_id = input_records[0].id
    output_embedding_id = output_records[0].id
    output_order = [{"index": 0, "embedding_id": output_embedding_id},
                    {"index": 1, "embedding_id": output_embedding_id}]
    return True, input_embedding_id, output_order

async def init_triton_mappers(db, input_sizes, output_sizes):
    current_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_triton',
                                                                'type' : 'mapper'})
    current_record_names = [i.name for i in current_records]
    for input_size in input_sizes:
        for output_size in output_sizes:
            create_config = triton_mapper_config(input_size, output_size)
            if create_config["name"] in current_record_names:
                continue 

            logging.info(f"Creating triton mapper record {create_config['name']}")
            valid, input_embedding_id, output_order = await get_mapper_embeddings(db, input_size, output_size)
            if not valid:
                continue 

            create_config["input_embedding_id"] = input_embedding_id
            create_config["output_order"] = output_order 
            logging.info(f"Creating Mapper record on backend")
            record = schemas.MapperPluginCreate(**create_config)
            response = await crud.create_plugin(db=db, plugin=record)
            logging.info(response)
            logging.info(f"Successfully created triton mapper record {response.id}")

async def init_triton_records(db):
    logging.info(f"Creating triton records")
    try:
        logging.info("Polling triton server for model sizes")
        size_data = await get_triton_model_sizes()
    except Exception as e:
        logging.warning(f"Failed to contact triton server, aborting")
        logging.warning(f"Triton connect error: {e}")
        return 
    
    await init_triton_embeddings(db, size_data["mapper_input_sizes"])
    await init_triton_mappers(db, size_data["mapper_input_sizes"], size_data["mapper_output_sizes"])

