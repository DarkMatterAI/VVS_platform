import os 
from app import crud 
from app import schemas, utils 

from vvs_database import logging 

TEI_EMBEDDING = {
    "name": f"TEI Embedding {os.environ.get('TEI_MODEL_ID', '')}",
    "type": "embedding",
    "plugin_class": "internal_tei",
    "execution_type" : "api",
    "group_key": "tei_plugin",
    "timeout": 600,
    "max_concurrency": int(os.environ.get("TEI_REQUEST_CONCURRENCY", 24)),
    "batch_size": int(os.environ.get("TEI_MAX_CLIENT_BATCH_SIZE", 512)),
    "max_retries": 3,
    "endpoint_url": f"http://tei_plugin:{os.environ.get('TEI_PORT', '')}/embed",
    "vector_length": int(os.environ.get("TEI_EMBEDDING_SIZE")),
    "distance_metric": os.environ.get("TEI_DISTANCE_METRIC", ""),
    "config": {
        "normalize": False if os.environ.get("TEI_NORMALIZE", "false")=="false" else True,
        "truncate": False if os.environ.get("TEI_TRUNCATE", "false")=="false" else True,
        "truncation_direction": os.environ.get("TEI_TRUNCATION_DIRECTION", "right")
    }
}


async def init_tei_records(db):
    current_tei_records = await crud.get_plugins(db, filter_params={'plugin_class' : 'internal_tei'})
    found_existing = False 
    for record in current_tei_records:
        if record.name == TEI_EMBEDDING['name']:
            found_existing = True 
            continue 

        num_linked_records = await crud.count_plugins_linked_to_embedding_id(db, record.id)
        logging.warning(f"Found stale TEI plugin {record.id} " \
                       f"with {num_linked_records} linked records")

    if found_existing:
        return 
    
    logging.info("Creating TEI embedding record")
    try:
        _ = schemas.DistanceMetric(TEI_EMBEDDING['distance_metric'])
    except:
        logging.warning(f"TEI embedding invalid distance metric {TEI_EMBEDDING['distance_metric']} - skipping")
        return 

    record = schemas.EmbeddingPluginCreate(**TEI_EMBEDDING)
    try:
        response = await crud.create_plugin(db=db, plugin=record)
        logging.info(f"Successfully created TEI record {response.id}")
    except:
        logging.warning(f"Creating TEI record failed")