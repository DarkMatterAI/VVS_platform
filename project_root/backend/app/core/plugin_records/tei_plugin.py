import os 
import logging 

from app import crud 
from app import schemas, utils 

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)



TEI_EMBEDDING = {
    "name": f"TEI Embedding {os.environ.get('TEI_MODEL_ID', '')}",
    "type": "embedding",
    "plugin_class": "internal_tei",
    "execution_type" : "api",
    "group_key": "tei_plugin",
    "timeout": 600,
    "max_concurrency": int(os.environ.get('TEI_REQUEST_CONCURRENCY', 24)),
    "batch_size": int(os.environ.get('TEI_MAX_CLIENT_BATCH_SIZE', 256)),
    "max_retries": 2,
    "endpoint_url" : f"http://plugin_integration_server:{os.environ.get('PLUGIN_INTEGRATION_SERVER_PORT')}/tei_embed",
    "vector_length": None,
    "distance_metric": os.environ.get('TEI_DISTANCE_METRIC', ''),
    "config": {}
}


async def init_tei_records(db):
    current_tei_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_tei'})
    found_existing = False 
    for record in current_tei_records:
        if record.name == TEI_EMBEDDING['name']:
            found_existing = True 
            continue 

        num_linked_records = await crud.count_plugins_linked_to_embedding_id(db, record.id)
        logger.warning(f"Found stale TEI plugin {record.id} " \
                       f"with {num_linked_records} linked records")

    if found_existing:
        return 
    
    print("Creating TEI embedding record")
    try:
        _ = schemas.DistanceMetric(TEI_EMBEDDING['distance_metric'])
    except:
        logger.warning(f"TEI embedding invalid distance metric {TEI_EMBEDDING['distance_metric']} - skipping")
        return 

    print("Checking TEI vector size")    
    try:
        request_data = {
            'request_data' : {
                'request_id' : '',
                'plugin_id' : 1,
                'plugin_name' : ''
            },
            'item_data' : {
                'item_id' : 1,
                'external_id' : '1',
                'item' : '.',
                'embedding' : None 
            }
        }
        response = await utils.fastapi_post_request(request_data,
                                                 TEI_EMBEDDING['endpoint_url'],
                                                 timeout=10, retries=20, retry_sleep=1)
    except:
        logger.warning(f"Request to TEI server failed - aborting")
        return 

    TEI_EMBEDDING['vector_length'] = len(response['embedding'])

    record = schemas.EmbeddingPluginCreate(**TEI_EMBEDDING)
    try:
        response = await crud.create_plugin(db=db, plugin=record)
        print(f"Successfully created TEI record {response.id}")
    except:
        logger.warning(f"Creating TEI record failed")