from aioredis import Redis
import logging 

from app import utils
from app import crud 
from vvs_database import logging 
from vvs_database.settings import settings 
from app.core.database import get_db, launch_config 
from app.core.plugin_records.records_config import PLUGIN_CREATE_DICT

async def _init_records(db):
    config = launch_config['plugins']
    # config = utils.read_config()['plugins']
    
    for plugin_name, plugin_data in config.items():
        if plugin_name not in PLUGIN_CREATE_DICT:
            logging.warning(f"Plugin {plugin_name} enambed in config but " \
                            f"no initial records were found")
            continue 

        plugin_enabled = plugin_data.get('enabled', False)
        plugin_class = PLUGIN_CREATE_DICT[plugin_name]['plugin_class']
        current_record_count = await crud.count_plugins_by_class(db, plugin_class)
        if (not plugin_enabled) and (current_record_count>0):
            if (plugin_name=='tei_plugin') or (plugin_name=='triton_plugin'):
                linked_record_count = await crud.count_plugins_linked_to_embedding_class(db, plugin_class)
                logging.warning(f"Plugin {plugin_name} is disabled but " \
                                f"{current_record_count} records exist in the database " \
                                f"with {linked_record_count} linked records impacted")
            else:
                logging.warning(f"Plugin {plugin_name} is disabled but " \
                               f"{current_record_count} records exist in the database")
                
        if plugin_enabled:
            await PLUGIN_CREATE_DICT[plugin_name]['create_func'](db)

async def init_records():
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    lock = redis.lock("records_init_lock", timeout=60)

    try:
        logging.info('Acquiring redis lock for records init')
        await lock.acquire()
        async for db in get_db():
            await _init_records(db)
            break 
    finally:
        logging.info('Releasing redis lock')
        await lock.release()
        await redis.close()
            

