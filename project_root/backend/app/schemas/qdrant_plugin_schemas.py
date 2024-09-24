from qdrant_client import models

from pydantic import BaseModel, Field, model_validator
from typing import Optional, List, Dict, Union, Mapping

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




# 1. validate config
#   1.1 check vectors config matches embedding ids 
# 2. create collection
#   2.1 Pull down embedding records
#   2.2 make qdrant create data
#   2.3 create collection
#   2.4 get collection info
# 3. create db record
#   3.1 create db plugin create format with collection info as info
#   3.2 hit crud endpoint
# 4. return result 
