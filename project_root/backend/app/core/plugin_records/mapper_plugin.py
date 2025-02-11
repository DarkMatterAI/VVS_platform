import os 
import logging 

from app.crud import plugin_crud as crud 
from app import schemas 

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

TRITON_MAPPER = {
    "name": f"Triton mapper",
    "type": "mapper",
    "plugin_class": "internal_mapper",
    "execution_type" : "api",
    "group_key": "mapper_plugin",
    "timeout": 600,
    "max_concurrency": 64,
    "max_retries": 2,
    "input_embedding_id": None,
    "output_order": [],
    "endpoint_url" : f"http://plugin_integration_server:{os.environ.get('PLUGIN_INTEGRATION_SERVER_PORT')}/mapper_plugin",
    "config": {}
}

MAPPER_MODEL_NAME="entropy/roberta_zinc_480m"

async def init_mapper_records(db):
    current_mapper_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_mapper'})
    if current_mapper_records:
        return 

    print("Creating mapper plugin record")

    print(f"Looking for embedding record matching mode; name {MAPPER_MODEL_NAME}")
    embedding_records = await crud.get_plugins(db, filter_params={'type' : 'embedding',
                                                                  'name' : f"%{MAPPER_MODEL_NAME}%"})
    if not embedding_records:
        logger.warning(f"Mapper create unable to find embedding record for model {MAPPER_MODEL_NAME} - aborting")
        return 
    
    embedding_id = embedding_records[0].id 
    TRITON_MAPPER["input_embedding_id"] = embedding_id
    TRITON_MAPPER["output_order"] = [{'index':0, 'embedding_id':embedding_id},
                                     {'index':1, 'embedding_id':embedding_id}]

    print(f"Creating Mapper record on backend")
    record = schemas.MapperPluginCreate(**TRITON_MAPPER)
    response = await crud.create_plugin(db=db, plugin=record)
    print(response)
