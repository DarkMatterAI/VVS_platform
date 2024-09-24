import os 
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
        response = await client.delete_collection(collection_name)
        return response 


