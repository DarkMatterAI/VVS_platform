from app import crud 
from app.crud.qdrant_utils import (get_collection_names, 
                                   qdrant_get_collection, 
                                   qdrant_create, 
                                   restore_snapshot,
                                   qdrant_delete_collection)
from vvs_database import logging

async def init_qdrant_records(db):
    collection_names = await get_collection_names(retries=10)
    found_collections = set()
    if collection_names is None:
        logging.warning(f"Qdrant get collection names failed - aborting")
        return 
    
    logging.info(f"Found {len(collection_names)} qdrant collections, checking postgres")
    skip = 0
    limit = 100
    while True:
        qdrant_records = await crud.get_plugins(db, {'plugin_class' : 'internal_qdrant'}, skip, limit)

        if not qdrant_records:
            break 

        logging.info(f"Validating {len(qdrant_records)} records")
        for record in qdrant_records:
            collection_name = f"data_source_{record.id}"
            if collection_name in collection_names:
                found_collections.update([collection_name])
                continue 

            logging.info(f"Found record {record.id} without collection, looking for snapshot")
            snapshot_data = record.config['snapshot']
            if snapshot_data is None:
                logging.info(f"No snapshot for record {record.id}, creating collection")
                try:
                    collection_info = await qdrant_create(record, record.config['qdrant_config'])
                    config = dict(record.config)
                    config['collection_info'] = collection_info
                    record.config = config
                    await db.commit()
                    await db.refresh(record)
                    found_collections.update([collection_name])
                except Exception as e:
                    logging.warning(f"Create collection for record {record.id} failed - {e}")
            else:
                logging.info(f"Restoring snapshot {snapshot_data['name']} for {record.id}")
                try:
                    snapshot_response = await restore_snapshot(record, snapshot_data['name'])
                    if not snapshot_response:
                        logging.info(f"Restoring snapshot returned False from Qdrant")
                    else:
                        collection_info = await qdrant_get_collection(record)
                        config = dict(record.config)
                        config['collection_info'] = collection_info
                        record.config = config
                        await db.commit()
                        await db.refresh(record)
                        found_collections.update([collection_name])
                except Exception as e:
                    logging.warning(f"Restore snapshot for record {record.id} failed - {e}")

        skip = skip + limit 

    missing_collections = [i for i in collection_names if i not in found_collections]
    if missing_collections:
        logging.info(f"Found {len(missing_collections)} collections in qdrant with no record: {missing_collections}")
