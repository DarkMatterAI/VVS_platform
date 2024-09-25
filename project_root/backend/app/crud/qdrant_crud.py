import os 
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import HTTPException

from app import schemas 

from app.crud import plugin_crud, qdrant_utils 

async def create_qdrant(db: AsyncSession, plugin: schemas.QdrantDataSourceCreate):
    print('Parsing qdrant create')
    plugin_data = plugin.model_dump(exclude_unset=True)
    qdrant_config = plugin_data['qdrant_config']
    embedding_ids = [i['embedding_id'] for i in qdrant_config['vectors_config']]

    print("Pulling embedding records")
    embedding_records = await plugin_crud.get_embeddings(db, embedding_ids)
    embedding_record_dict = {i.id:i for i in embedding_records}

    print('Validating embedding ids')
    if len(embedding_records) != len(embedding_ids):
        invalid_ids = set(embedding_ids) - set(e.id for e in embedding_records)
        raise HTTPException(status_code=400, detail=f"Invalid embedding IDs: {invalid_ids}")

    print('Validating create record')
    create_record = {
        'name' : plugin_data['name'],
        'type' : 'data_source',
        "plugin_class" : "internal_qdrant",
        'embedding_ids' : embedding_ids,
        # 'execution_type' : 'internal_qdrant',
        "execution_type" : "api",
        'timeout' : int(os.environ.get('QDRANT_QUERY_TIMEOUT', 30)),
        'max_concurrency' : int(os.environ.get('QDRANT_QUERY_CONCURRENCY', 256)),
        'max_retries' : int(os.environ.get('QDRANT_QUERY_MAX_RETRIES', 4)),
        'endpoint_url' : 'placeholder',
        'group_key' : 'qdrant_plugin',
        'config' : {}
    }
    create_record = schemas.DataSourcePluginCreate(**create_record)

    print("Creating on backend")
    create_record = await plugin_crud.create_plugin(db, create_record)

    print("Creating qdrant create record")
    vectors_config = qdrant_config.pop('vectors_config')
    vector_create_config = {}
    for vector_config in vectors_config:
        embedding_id = vector_config.pop('embedding_id')
        embedding_record = embedding_record_dict[embedding_id]
        vector_config['size'] = embedding_record.vector_length
        vector_config['distance'] = embedding_record.distance_metric
        vector_create_config[f"embedding_{embedding_id}"] = vector_config

    qdrant_config['vectors_config'] = vector_create_config
    print(qdrant_config)

    print("Creating on qdrant")
    try:
        collection_info = await qdrant_utils.qdrant_create(create_record, qdrant_config)
    except Exception as e:
        print(f"Qdrant create failed: {e}")
        print("Deleting record")
        await plugin_crud.delete_plugin(db, create_record.id)
        raise HTTPException(status_code=502, 
                            detail=f"Qdrant create failed with on qdrant side with exception {e}")
    
    print("Updating record config")
    record_config = {'qdrant_config' : qdrant_config, 
                     'collection_info' : collection_info,
                     'snapshot' : None 
                     }
    setattr(create_record, 'config', record_config)

    endpoint_url = f"http://plugin_integration_server:{os.environ.get('PLUGIN_INTEGRATION_SERVER_PORT')}"
    endpoint_url = f"{endpoint_url}/data_source_qdrant/data_source_{create_record.id}"
    setattr(create_record, 'endpoint_url', endpoint_url)

    await db.commit()
    await db.refresh(create_record)
    return create_record.id 

async def delete_qdrant(db: AsyncSession, db_plugin):
    plugin_id = db_plugin.id 
    
    print("Deleting qdrant collection")
    try:
        delete_response = await qdrant_utils.qdrant_delete(db_plugin)
        print(delete_response)
        assert delete_response 
    except Exception as e:
        raise HTTPException(status_code=502, 
                            detail=f"Qdrant delete failed with exception {e}")

    print("Deleting database record")
    response = await plugin_crud.delete_plugin(db, db_plugin)
    return response 


async def update_collection_data(db: AsyncSession, plugin_id: int):
    db_plugin = await plugin_crud.get_plugin(db, plugin_id)
    if not db_plugin:
        return None 
    
    print("Getting collection data")
    try:
        collection_info = await qdrant_utils.qdrant_get_collection(db_plugin)
    except Exception as e:
        raise HTTPException(status_code=502, 
                            detail=f"Qdrant get collection failed with exception {e}")
    
    config = dict(db_plugin.config)
    config['collection_info'] = collection_info
    setattr(db_plugin, 'config', config)
    await db.commit()
    await db.refresh(db_plugin)
    return db_plugin.id 

async def update_snapshot(db: AsyncSession, plugin_id: int, snapshot_data: schemas.QdrantSnapshotData):
    db_plugin = await plugin_crud.get_plugin(db, plugin_id)
    if not db_plugin:
        return None 
    
    config = dict(db_plugin.config)
    config['snapshot'] = snapshot_data.model_dump()
    setattr(db_plugin, 'config', config)
    await db.commit()
    await db.refresh(db_plugin)
    return db_plugin.id 
