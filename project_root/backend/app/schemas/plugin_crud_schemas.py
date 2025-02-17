from pydantic import (
                        BaseModel, 
                        RootModel, 
                        Field, 
                        field_validator,
                        model_validator, 
                        ValidationError,
                        )
from typing import Optional, List, Union, Dict
from enum import Enum
from datetime import datetime

class PluginType(str, Enum):
    EMBEDDING = 'embedding'
    DATA_SOURCE = 'data_source'
    FILTER = 'filter'
    SCORE = 'score'
    MAPPER = 'mapper'
    ASSEMBLY = 'assembly'

class PluginClass(str, Enum):
    GENERIC = 'generic'
    INTERNAL_RDKIT = 'internal_rdkit'
    INTERNAL_TEI = 'internal_tei'
    INTERNAL_QDRANT = 'internal_qdrant'
    INTERNAL_TRITON = 'internal_triton'

class PluginExecutionType(str, Enum):
    QUEUE = "queue"
    API = "api"

class DistanceMetric(str, Enum):
    Cosine = 'Cosine'
    Euclid = 'Euclid'
    Dot = 'Dot'

class PluginBase(BaseModel):
    name: str
    type: PluginType
    plugin_class: PluginClass
    execution_type: PluginExecutionType
    timeout: Optional[int] = None
    max_concurrency: Optional[int] = None
    max_retries: Optional[int] = None
    batch_size: Optional[int] = None
    endpoint_url: Optional[str] = None
    group_key: Optional[str] = None
    config: Optional[Dict] = None
    plugin_metadata: Optional[Dict] = None

    @field_validator('timeout', 'max_concurrency', 'max_retries', 'group_key')
    def check_queue_api_fields(cls, v, info):
        if v is None:
            raise ValueError(f"{info.field_name} is required")
        return v
    
    @field_validator('batch_size')
    def check_batch_size(cls, v, info):
        if (type(v)==int) and (v <= 0):
            raise ValueError(f"{info.field_name} must be greater than zero")
        return v

    @field_validator('endpoint_url')
    def check_api_fields(cls, v, info):
        if info.data.get('execution_type') == PluginExecutionType.API:
            if v is None:
                raise ValueError("endpoint_url is required for API execution type")
        return v

    @model_validator(mode='after')
    def check_consistency(self):
        if any(field is None for field in [self.timeout, self.max_concurrency, self.max_retries, self.group_key]):
            raise ValueError("timeout, max_concurrency, and max_retries are required")
        
        if self.execution_type == PluginExecutionType.API and self.endpoint_url is None:
            raise ValueError("endpoint_url is required for API execution type")
        
        return self

class EmbeddingPluginCreate(PluginBase):
    type: PluginType = PluginType.EMBEDDING
    vector_length: int
    distance_metric: DistanceMetric

class DataSourcePluginCreate(PluginBase):
    type: PluginType = PluginType.DATA_SOURCE
    embedding_ids: List[int] = Field(..., min_items=1)

class FilterPluginCreate(PluginBase):
    type: PluginType = PluginType.FILTER
    embedding_ids: Optional[List[int]] = None

class ScorePluginCreate(PluginBase):
    type: PluginType = PluginType.SCORE
    embedding_ids: Optional[List[int]] = None

class OutputEmbedding(BaseModel):
    index: int 
    embedding_id: int 

class MapperPluginCreate(PluginBase):
    type: PluginType = PluginType.MAPPER
    input_embedding_id: int
    output_order: List[OutputEmbedding] = Field(..., min_items=2)

class AssemblyPluginCreate(PluginBase):
    type: PluginType = PluginType.ASSEMBLY
    num_parents: int = Field(..., gt=0)

    @field_validator('num_parents')
    def check_num_parents(cls, v, info):
        if v < 2:
            raise ValueError(f"Assembly expected at least 2 parents, found {v}")
        return v

PluginCreateUnion = Union[
    EmbeddingPluginCreate,
    DataSourcePluginCreate,
    FilterPluginCreate,
    ScorePluginCreate,
    MapperPluginCreate,
    AssemblyPluginCreate
]

create_type_mapping = {i.model_fields['type'].default:i for i in PluginCreateUnion.__args__}

class PluginCreate(RootModel):
    root: PluginCreateUnion

    @model_validator(mode='before')
    @classmethod
    def validate_type(cls, values):
        if isinstance(values, dict) and 'type' in values:
            plugin_type = values['type']
            try:
                _ = PluginType(plugin_type)
                output = create_type_mapping[plugin_type](**values)
                return output 
            except ValidationError as e:
                raise ValueError(f"Invalid data for plugin type {plugin_type} - {e}")
        raise ValueError("Invalid or missing 'type' field")

class PluginUpdate(BaseModel):
    name: Optional[str] = None
    execution_type: Optional[PluginExecutionType] = None
    timeout: Optional[int] = None
    max_concurrency: Optional[int] = None
    max_retries: Optional[int] = None
    batch_size: Optional[int] = None
    endpoint_url: Optional[str] = None
    group_key: Optional[str] = None
    config: Optional[Dict] = None
    plugin_metadata: Optional[Dict] = None

    vector_length: Optional[int] = None
    distance_metric: Optional[DistanceMetric] = None
    embedding_ids: Optional[List[int]] = None
    input_embedding_id: Optional[int] = None
    output_order: Optional[List[OutputEmbedding]] = None
    num_parents: Optional[int] = None

    @field_validator('timeout', 'max_concurrency', 'max_retries', 'group_key')
    def check_queue_api_fields(cls, v, info):
        if v is None:
            raise ValueError(f"{info.field_name} is required")
        return v
    
    @field_validator('batch_size')
    def check_batch_size(cls, v, info):
        if (type(v)==int) and (v <= 0):
            raise ValueError(f"{info.field_name} must be greater than zero")
        return v

    @field_validator('endpoint_url')
    def check_api_fields(cls, v, info):
        if 'execution_type' in info.data:
            if info.data['execution_type'] == PluginExecutionType.API:
                if v is None:
                    raise ValueError("endpoint_url is required for API execution type")
        return v

    @model_validator(mode='after')
    def check_consistency(self):
        if self.execution_type == PluginExecutionType.API and self.endpoint_url is None:
            raise ValueError("endpoint_url is required for API execution type")

        return self

class PluginInDB(PluginBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class EmbeddingPluginInDB(PluginInDB, EmbeddingPluginCreate):
    pass

class DataSourcePluginInDB(PluginInDB, DataSourcePluginCreate):
    pass

class FilterPluginInDB(PluginInDB, FilterPluginCreate):
    pass

class ScorePluginInDB(PluginInDB, ScorePluginCreate):
    pass

class MapperPluginInDB(PluginInDB, PluginBase):
    embedding_ids: Optional[List[int]] = None
    input_embedding_id: int
    output_order: List[OutputEmbedding]

class AssemblyPluginInDB(PluginInDB, AssemblyPluginCreate):
    pass

PluginInDBUnion = Union[
    EmbeddingPluginInDB,
    DataSourcePluginInDB,
    FilterPluginInDB,
    ScorePluginInDB,
    MapperPluginInDB,
    AssemblyPluginInDB
]

