import os 
import logging 
import asyncio 

from app.crud import plugin_crud as crud 
from app.crud.qdrant_utils import (get_collection_names, 
                                   qdrant_get_collection, 
                                   qdrant_create, 
                                   restore_snapshot)
from app import schemas, utils 

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

RDKIT_FILTERS = [
    {
        "name": "Rule of 5 Filter",
        "type": "filter",
        "execution_type": "queue",
        "group_key": "rdkit_plugin",
        "timeout": 600,
        "max_concurrency": 64,
        "max_retries": 2,
        "config" : {
            "property_filters" : [
                {"property_name" : "LogP", "Min_val" : None, "max_val" : 5.0},
                {"property_name" : "Molecular Weight", "Min_val" : None, "max_val" : 500.0},
                {"property_name" : "Hydrogen Bond Donors", "Min_val" : None, "max_val" : 5.0},
                {"property_name" : "Hydrogen Bond Acceptors", "Min_val" : None, "max_val" : 5.0},
            ]
        }
    }
]


async def init_rdkit_records(db):
    for record in RDKIT_FILTERS:
        current_record = await crud.get_plugins(db, filter_params={'name' : record['name']})
        if not current_record:
            print(f"Creating RDKit record {record['name']}")
            record = schemas.FilterPluginCreate(**record)
            response = await crud.create_plugin(db=db, plugin=record)
            print(response)

TEI_EMBEDDING = {
    "name": f"TEI Embedding {os.environ.get('TEI_MODEL_ID', '')}",
    "type": "embedding",
    "execution_type": "internal_tei",
    "group_key": "tei_plugin",
    "timeout": 600,
    "max_concurrency": 64,
    "max_retries": 2,
    "endpoint_url": f"http://tei_plugin:{os.environ.get('TEI_PORT', '')}/embed",
    "vector_length": None,
    "distance_metric": os.environ.get('TEI_DISTANCE_METRIC', ''),
    "config" : {'normalize' : False if os.environ.get('TEI_NORMALIZE', 'false')=='false' else True,
                'truncate' : False if os.environ.get('TEI_TRUNCATE', 'false')=='false' else True,
                'truncation_direction' : os.environ.get('TEI_TRUNCATION_DIRECTION', 'right')}
}


async def init_tei_records(db):
    current_tei_records = await crud.get_plugins(db, filter_params={'group_key' : 'tei_plugin'})
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
        response = await utils.make_post_request(TEI_EMBEDDING['endpoint_url'], {'inputs' : '.'},
                                                 timeout=10, retries=20, retry_sleep=1)
    except:
        logger.warning(f"Request to TEI server failed - aborting")
        return 

    TEI_EMBEDDING['vector_length'] = len(response[0])

    record = schemas.EmbeddingPluginCreate(**TEI_EMBEDDING)
    try:
        response = await crud.create_plugin(db=db, plugin=record)
        print(f"Successfully created TEI record {response.id}")
    except:
        logger.warning(f"Creating TEI record failed")

async def init_qdrant_records(db):
    collection_names = await get_collection_names(retries=10)
    found_collections = set()
    if collection_names is None:
        logger.warning(f"Qdrant get collection names failed - aborting")
        return 
    
    print(f"Found {len(collection_names)} qdrant collections, checking postgres")
    skip = 0
    limit = 100
    while True:
        qdrant_records = await crud.get_plugins(db, {'group_key' : 'qdrant_plugin'}, skip, limit)

        if not qdrant_records:
            break 

        print(f"Validating {len(qdrant_records)} records")
        for record in qdrant_records:
            collection_name = f"data_source_{record.id}"
            if collection_name in collection_names:
                found_collections.update([collection_name])
                continue 

            print(f"Found record {record.id} without collection, looking for snapshot")
            snapshot_data = record.config['snapshot']
            if snapshot_data is None:
                print(f"No snapshot for record {record.id}, creating collection")
                try:
                    collection_info = await qdrant_create(record, record.config['qdrant_config'])
                    config = dict(record.config)
                    config['collection_info'] = collection_info
                    record.config = config
                    await db.commit()
                    await db.refresh(record)
                    found_collections.update([collection_name])
                except Exception as e:
                    print(f"Create collection for record {record.id} failed - {e}")
            else:
                print(f"Restoring snapshot {snapshot_data['name']} for {record.id}")
                try:
                    snapshot_response = await restore_snapshot(record, snapshot_data['name'])
                    if not snapshot_response:
                        print(f"Restoring snapshot returned False from Qdrant")
                    else:
                        collection_info = await qdrant_get_collection(record)
                        config = dict(record.config)
                        config['collection_info'] = collection_info
                        record.config = config
                        await db.commit()
                        await db.refresh(record)
                        found_collections.update([collection_name])
                except Exception as e:
                    print(f"Restore snapshot for record {record.id} failed - {e}")

        skip = skip + limit 

    missing_collections = [i for i in collection_names if i not in found_collections]
    if missing_collections:
        print(f"Found {len(missing_collections)} collections in qdrant with no record: {missing_collections}")



PLUGIN_CREATE_DICT = {
    'tei_plugin' : {
        'group_key' : 'tei_plugin',
        'create_func' : init_tei_records
    },
    'qdrant_plugin' : {
        'group_key' : 'qdrant_plugin',
        'create_func' : init_qdrant_records
    },
    'rdkit_plugin' : {
        'group_key' : 'rdkit_plugin',
        'create_func' : init_rdkit_records
    }
}

