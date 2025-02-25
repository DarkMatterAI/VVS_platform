from qdrant_client import models

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Union, Mapping

class QdrantSnapshotData(BaseModel):
    name: str 
    creation_time: str 
    size: int 
    checksum: str 

class QdrantVectorParams(BaseModel):
    embedding_id: int 
    hnsw_config: Optional[models.HnswConfigDiff] = None
    quantization_config: Union[models.ScalarQuantization, models.ProductQuantization, models.BinaryQuantization, None] = None
    on_disk: Optional[bool] = False
    datatype: Optional[models.Datatype] = None
    
class QdrantConfig(BaseModel):
    vectors_config: List[QdrantVectorParams] = Field(..., min_items=1)
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

class QdrantDataSourceCreate(BaseModel):
    name: str 
    qdrant_config: QdrantConfig


