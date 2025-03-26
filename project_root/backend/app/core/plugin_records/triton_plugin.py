import os 

from app import crud 
from app import schemas 

from vvs_database import logging 

EMBEDDING_SIZES = [768, 512, 256, 128, 64, 32]
MAPPER_SIZES = [64]
INTEGRATION_URL = f"http://plugin_integration_server:{os.environ.get('PLUGIN_INTEGRATION_SERVER_PORT')}"

TRITON_EMBEDDINGS = [
    {
        "name": f"Triton Embedding size {size}",
        "type": "embedding",
        "plugin_class": "internal_triton",
        "execution_type" : "api",
        "group_key": "triton_plugin_embedding",
        "timeout": 600,
        "max_concurrency": os.environ.get('TRITON_REQUEST_CONCURRENCY', 24),
        "batch_size": int(os.environ.get('TRITON_EMBED_BATCH_SIZE', 1024))//2,
        "max_retries": 2,
        "endpoint_url" : f"{INTEGRATION_URL}/triton_embed/EMBED_{size}",
        "vector_length": size,
        "distance_metric": 'Cosine',
        "config": {}
    }
    for size in EMBEDDING_SIZES
]

TRITON_MAPPERS = [
    {
        "name": f"Triton Mapper size {size}",
        "type": "mapper",
        "plugin_class": "internal_triton",
        "execution_type" : "api",
        "group_key": "triton_plugin_mapper",
        "timeout": 600,
        "max_concurrency": os.environ.get('TRITON_REQUEST_CONCURRENCY', 24),
        "batch_size": int(os.environ.get('TRITON_MAPPER_BATCH_SIZE', 1024))//2,
        "max_retries": 2,
        "input_embedding_id": None,
        "output_order": [],
        "endpoint_url" : f"{INTEGRATION_URL}/triton_map/ENAMINE_MAPPER_{size}",
        "config": {}
    }
    for size in MAPPER_SIZES
]


async def init_triton_embeddings(db):
    current_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_triton',
                                                                'type' : 'embedding'})
    current_record_names = [i.name for i in current_records]
    for embedding_record in TRITON_EMBEDDINGS:
        if embedding_record['name'] in current_record_names:
            continue 

        logging.info(f"Creating triton embedding record {embedding_record['name']}")
        record = schemas.EmbeddingPluginCreate(**embedding_record)
        try:
            response = await crud.create_plugin(db=db, plugin=record)
            logging.info(f"Successfully created triton embedding record {response.id}")
        except:
            logging.warning(f"Creating triton record failed")

async def init_triton_mappers(db):
    current_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_triton',
                                                                'type' : 'mapper'})
    current_record_names = [i.name for i in current_records]
    for mapper_record, embedding_size in zip(TRITON_MAPPERS, MAPPER_SIZES):
        if mapper_record['name'] in current_record_names:
            continue 

        logging.info(f"Creating triton mapper record {mapper_record['name']}")
        logging.info(f"Looking for embedding record matching mapper size {embedding_size}")
        embedding_records = await crud.get_plugins(db, filter_params={'type' : 'embedding',
                                                        'name' : f"%Triton Embedding size {embedding_size}%"})
        
        if not embedding_records:
            logging.warning(f"Mapper create unable to find embedding record - aborting")
            continue  

        embedding_id = embedding_records[0].id
        mapper_record["input_embedding_id"] = embedding_id
        mapper_record["output_order"] = [{'index':0, 'embedding_id':embedding_id},
                                         {'index':1, 'embedding_id':embedding_id}]
        logging.info(f"Creating Mapper record on backend")
        record = schemas.MapperPluginCreate(**mapper_record)
        response = await crud.create_plugin(db=db, plugin=record)
        logging.info(response)
        logging.info(f"Successfully created triton mapper record {response.id}")

async def init_triton_records(db):
    await init_triton_embeddings(db)
    await init_triton_mappers(db)



