import os 
import time 
from aioredis import Redis
import logging 

from app import schemas, utils
from app.crud import plugin_crud as crud 

from .database import get_db, REDIS_URL
from .init_records_configs import TEI_EMBEDDING, RDKIT_FILTERS, PLUGIN_CREATE_DICT

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# async def init_rdkit_records(db):
#     for record in RDKIT_FILTERS:
#         current_record = await crud.get_plugins(db, filter_params={'name' : record['name']})
#         if not current_record:
#             print(f"Creating RDKit record {record['name']}")
#             record = schemas.FilterPluginCreate(**record)
#             response = await crud.create_plugin(db=db, plugin=record)
#             print(response)

# async def init_tei_records(db):
#     current_record = await crud.get_plugins(db, filter_params={'group_key' : 'tei_plugin'})
#     if current_record:
#         current_record = current_record[0]
#         if current_record.name == TEI_EMBEDDING['name']:
#             return 
#         else:
#             print(f"Stale TEI record found, deleting")
#             await crud.delete_plugin(db, current_record.id)
#             current_record = None 

#     distance_metric = TEI_EMBEDDING['distance_metric']
#     try:
#         _ = schemas.DistanceMetric(distance_metric)
#     except:
#         print(f"TEI embedding invalid distance metric {distance_metric}")
#         return 
    
#     try:
#         response = await utils.make_post_request(TEI_EMBEDDING['endpoint_url'], {'inputs' : '.'},
#                                                  timeout=10, retries=10, retry_sleep=1)
#     except:
#         print(f"Request to TEI server failed - aborting")
#         return 

#     TEI_EMBEDDING['vector_length'] = len(response[0])

#     if not current_record:
#         print("Creating TEI record")
#         record = schemas.EmbeddingPluginCreate(**TEI_EMBEDDING)
#         response = await crud.create_plugin(db=db, plugin=record)
#         print(response)

# async def _init_records(db):
#     config = utils.read_config()['plugins']

#     if config.get('rdkit_plugin', {}).get('enabled', False):
#         print('Creating rdkit plugin records')
#         await init_rdkit_records(db)

#     if config.get('tei_plugin', {}).get('enabled', False):
#         print('Creating TEI embedding record')
#         await init_tei_records(db)

# async def init_records():
#     redis = Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
#     lock = redis.lock("records_init_lock", timeout=60)

#     try:
#         print('Acquiring redis lock for records init')
#         await lock.acquire()
#         async for db in get_db():
#             await _init_records(db)
#             break 
#     finally:
#         print('Releasing redis lock')
#         await lock.release()
#         await redis.close()

async def _init_records(db):
    config = utils.read_config()['plugins']
    
    for plugin_name, plugin_data in config.items():
        plugin_enabled = plugin_data.get('enabled', False)
        plugin_group_key = PLUGIN_CREATE_DICT[plugin_name]['group_key']
        current_record_count = await crud.count_plugins_by_group_key(db, plugin_group_key)
        if (not plugin_enabled) and (current_record_count>0):
            if plugin_name=='tei_plugin':
                linked_record_count = await crud.count_plugins_linked_to_embedding_group(db, plugin_group_key)
                logger.warning(f"Plugin {plugin_name} is disabled but " \
                               f"{current_record_count} records exist in the database " \
                               f"with {linked_record_count} linked records impacted")
            else:
                logger.warning(f"Plugin {plugin_name} is disabled but " \
                               f"{current_record_count} records exist in the database")
                
        if plugin_enabled:
            await PLUGIN_CREATE_DICT[plugin_name]['create_func'](db)

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
            

