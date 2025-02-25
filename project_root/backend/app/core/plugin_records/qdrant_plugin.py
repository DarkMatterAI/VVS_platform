import logging 

from app import crud 
from app.crud.qdrant_utils import (get_collection_names, 
                                   qdrant_get_collection, 
                                   qdrant_create, 
                                   restore_snapshot)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

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
        qdrant_records = await crud.get_plugins(db, {'plugin_class' : 'internal_qdrant'}, skip, limit)

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
