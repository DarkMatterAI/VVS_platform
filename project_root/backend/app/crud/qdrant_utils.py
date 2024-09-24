import os 
import time 
from qdrant_client import AsyncQdrantClient, models
from fastapi import HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import Union, Mapping, Optional

class QdrantCreateConfig(BaseModel):
    vectors_config: Union[models.VectorParams, Mapping[str, models.VectorParams]]
    sparse_vectors_config: Optional[Mapping[str, models.SparseVectorParams]] = None
    shard_number: Optional[int] = None  
    sharding_method: Optional[models.ShardingMethod] = None
    replication_factor: Optional[int] = None
    write_consistency_factor: Optional[int] = None
    on_disk_payload: Optional[bool] = None
    hnsw_config: Optional[models.HnswConfigDiff] = None
    optimizers_config: Optional[models.OptimizersConfigDiff] = None
    wal_config: Optional[models.WalConfigDiff] = None
    quantization_config: Union[models.ScalarQuantization, models.ProductQuantization, models.BinaryQuantization, None] = None
    init_from: Optional[models.models.InitFrom] = None

CREATE_KWARGS = ['vectors_config', 
                 'sparse_vectors_config',
                 'shard_number',
                 'sharding_method',
                 'replication_factor',
                 'write_consistency_factor',
                 'on_disk_payload',
                 'hnsw_config',
                 'optimizers_config',
                 'wal_config',
                 'quantization_config',
                 'init_from'
                ]

@asynccontextmanager
async def get_qdrant_client():
    client = AsyncQdrantClient(location='qdrant', 
                               port=int(os.environ.get('QDRANT__SERVICE__HTTP_PORT', 6333)), 
                               grpc_port=int(os.environ.get('QDRANT__SERVICE__GRPC_PORT', 6334)),
                               prefer_grpc=True,
                               timeout=60
                               )
    try:
        yield client
    finally:
        await client.close()

async def qdrant_create(db_record, qdrant_config):
    async with get_qdrant_client() as client:
        collection_name = f"data_source_{db_record.id}"
        qdrant_config = QdrantCreateConfig(**qdrant_config)
        qdrant_config = {i:getattr(qdrant_config, i, None) for i in CREATE_KWARGS}
        response = await client.create_collection(collection_name=collection_name,
                                                  **qdrant_config)
        
        collection_info = await client.get_collection(collection_name)
        print('Qdrant create successful')
        return collection_info.model_dump()

async def qdrant_delete(db_record):
    async with get_qdrant_client() as client:
        collection_name = f"data_source_{db_record.id}"
        print(f"Deleting qdrant collection {collection_name}")
        response = await client.delete_collection(collection_name)
        print(f"Delete collection {collection_name} response: {response}")
        return response 

async def qdrant_get_collection(db_record):
    async with get_qdrant_client() as client:
        collection_name = f"data_source_{db_record.id}"
        response = await client.get_collection(collection_name)
        return response.model_dump()

async def get_collection_names(retries):
    # retries in case docker service hasn't started
    async with get_qdrant_client() as client:
        for i in range(retries):
            try:
                collections = await client.get_collections()
                collections = collections.model_dump()
                collection_names = [i['name'] for i in collections['collections']]
                return collection_names 
            except:
                print("Failed to connect to qdrant, sleeping")
                time.sleep(1)
        return None 

async def restore_snapshot(db_record, snapshot_name):
    async with get_qdrant_client() as client:
        collection_name = f"data_source_{db_record.id}"
        snapshot_path = f"{os.environ.get('QDRANT__STORAGE__SNAPSHOTS_PATH', 'qdrant_snapshots')}/{collection_name}"
        snapshot_location = f"file:///qdrant/{snapshot_path}/{snapshot_name}"
        print(f"Recovering snapshot file {snapshot_location}")
        snapshot_response = await client.recover_snapshot(collection_name, snapshot_location)
        return snapshot_response

async def qdrant_query(db_record, request):
    async with get_qdrant_client() as client:
        collection_name = f"data_source_{db_record.id}"
        embedding_name = f"embedding_{request['embedding']['id']}"
        qdrant_results = await client.query_points(
            collection_name=collection_name,
            query=request['embedding']['embedding'],
            using=embedding_name,
            limit=request['k'],
            with_vectors=True
        )

        results = [] 
        for result in qdrant_results.points:
            result_data = {
                'external_id' : result.payload.get('external_id', 0),
                'item' : result.payload.get('item', ''),
                'embedding' : result.vector[embedding_name],
                'distance' : result.score
            }
            results.append(result_data)
        return results


