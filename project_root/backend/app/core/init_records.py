import os 
import time 
from aioredis import Redis

from app import schemas, utils
from app.crud import plugin_crud as crud 

from .database import get_db, REDIS_URL

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
    "config" : {'normalize' : os.environ.get('TEI_NORMALIZE', 'false')}
}


async def init_rdkit_records(db):
    for record in RDKIT_FILTERS:
        current_record = await crud.get_plugins(db, filter_params={'name' : record['name']})
        if not current_record:
            print(f"Creating RDKit record {record['name']}")
            record = schemas.FilterPluginCreate(**record)
            response = await crud.create_plugin(db=db, plugin=record)
            print(response)

async def init_tei_records(db):
    current_record = await crud.get_plugins(db, filter_params={'group_key' : 'tei_plugin'})
    if current_record:
        current_record = current_record[0]
        if current_record.name == TEI_EMBEDDING['name']:
            return 
        else:
            print(f"Stale TEI record found, deleting")
            await crud.delete_plugin(db, current_record.id)
            current_record = None 

    distance_metric = TEI_EMBEDDING['distance_metric']
    try:
        _ = schemas.DistanceMetric(distance_metric)
    except:
        print(f"TEI embedding invalid distance metric {distance_metric}")
        return 
    
    try:
        response = await utils.make_post_request(TEI_EMBEDDING['endpoint_url'], {'inputs' : '.'},
                                                 timeout=10, retries=10, retry_sleep=1)
    except:
        print(f"Request to TEI server failed - aborting")
        return 

    TEI_EMBEDDING['vector_length'] = len(response[0])

    if not current_record:
        print("Creating TEI record")
        record = schemas.EmbeddingPluginCreate(**TEI_EMBEDDING)
        response = await crud.create_plugin(db=db, plugin=record)
        print(response)

async def _init_records(db):
    config = utils.read_config()['plugins']

    if config.get('rdkit_plugin', {}).get('enabled', False):
        print('Creating rdkit plugin records')
        await init_rdkit_records(db)

    if config.get('tei_plugin', {}).get('enabled', False):
        print('Creating TEI embedding record')
        await init_tei_records(db)

async def init_records():
    redis = Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    lock = redis.lock("records_init_lock", timeout=60)

    try:
        print('Acquiring redis lock for records init')
        await lock.acquire()
        async for db in get_db():
            await _init_records(db)
            break 
    finally:
        print('Releasing redis lock')
        await lock.release()
        await redis.close()
