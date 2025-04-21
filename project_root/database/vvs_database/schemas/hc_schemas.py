from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Optional, Dict, List, Union, Tuple
from typing import Generic, TypeVar, Annotated
from enum import Enum

from vvs_database.schemas.internal_schemas import (ExecutePluginCreate, 
                                                   ExecuteDataSourceCreate, 
                                                   ExecutePlugin,
                                                   ExecuteDataSource,
                                                   Query, 
                                                   QueryEmbedding,
                                                   GradientEmbedding, 
                                                   InternalItem
                                                   )
from vvs_database.schemas.enums import DistanceMetric
from vvs_database.schemas.job_schemas import UserItem

T = TypeVar("T")

class UpdateType(str, Enum):
    GROUP_UPDATE='group_update'
    GLOBAL_UPDATE='global_update'
    
class LearningRate(BaseModel):
    learning_rate: List[float]
    assembly_index: int 
    
class HCAssembledUpdateParams(BaseModel):
    update_type: UpdateType
    distance_metric: DistanceMetric
    learning_rate: List[LearningRate]

    @field_validator("learning_rate", mode="after")
    def _check_assembly_indices(cls, v: List[LearningRate]):
        indices = sorted(i.assembly_index for i in v)
        if indices != list(range(len(indices))):
            raise ValueError(
                f"assembly_index values must be 0..{len(indices)-1} with no gaps; got {indices}"
            )
        return v
        
    def convert_internal(self):
        return self
        
class HCUpdateParams(HCAssembledUpdateParams):
    learning_rate: List[float]

    @field_validator("learning_rate", mode="after")
    def _check_assembly_indices(cls, v: List[float]):
        return v
        
    def convert_internal(self):
        data = self.model_dump()
        data['learning_rate'] = [LearningRate(learning_rate=data['learning_rate'],
                                              assembly_index=0)]
        return HCAssembledUpdateParams(**data)
    
class HCInferenceParams(BaseModel):
    inference_limit: Optional[int]=None
    time_limit: Optional[int]=None

class HCJobParams(HCInferenceParams):
    auto_execute: bool=False
        
class AssembledUserItem(UserItem):
    assembly_index: int 
        
class HCAssembedInputItem(HCInferenceParams):
    max_iterations: int
    items: List[AssembledUserItem]

    @field_validator("items", mode="after")
    def _check_assembly_indices(cls, v: List[AssembledUserItem]):
        indices = sorted(i.assembly_index for i in v)
        if indices != list(range(len(indices))):
            raise ValueError(
                f"assembly_index values must be 0..{len(indices)-1} with no gaps; got {indices}"
            )
        return v
    
    def convert_internal(self):
        return self

        
class HCInputItem(HCInferenceParams):
    max_iterations: int 
    item: UserItem 
        
    def convert_internal(self):
        data = self.model_dump()
        item = data.pop('item')
        data['items'] = [AssembledUserItem(**item, assembly_index=0)]
        return HCAssembedInputItem(**data)
    
class HCConfigCreateBase(BaseModel):
    filter_configs: Optional[List[ExecutePluginCreate]]=None
    score_config: ExecutePluginCreate
    embedding_configs: Optional[List[ExecutePluginCreate]]=None

class HCConfigCreate(HCConfigCreateBase):
    data_config: ExecuteDataSourceCreate
        
class HCAssembledConfigCreate(HCConfigCreateBase):
    data_configs: List[ExecuteDataSourceCreate]
    assembly_config: ExecutePluginCreate

    @field_validator("data_configs", mode="after")
    def _check_assembly_indices(cls, v: List[ExecuteDataSourceCreate]):
        
        indices = sorted(i.data_source_params.assembly_index for i in v)
        if indices != list(range(len(indices))):
            raise ValueError(
                f"assembly_index values must be 0..{len(indices)-1} with no gaps; got {indices}"
            )
        return v
        
class HCMapperConfigCreate(HCAssembledConfigCreate):
    mapper_config: ExecutePluginCreate

class HCJobBase(BaseModel):
    job_params: HCJobParams
    update_params: HCUpdateParams
    job_inputs: Annotated[List[HCInputItem], Field(min_length=1)]

class HCJobCreate(HCJobBase):
    plugin_config: HCConfigCreate
        
class HCMapperJobCreate(HCJobBase):
    plugin_config: HCMapperConfigCreate
        
class HCAssembledJobCreate(HCJobBase):
    plugin_config: HCAssembledConfigCreate
    update_params: HCAssembledUpdateParams
    job_inputs: Annotated[List[HCAssembedInputItem], Field(min_length=1)]

HCCreateConfigs = Union[HCConfigCreate, HCAssembledConfigCreate, HCMapperConfigCreate]

class HCSearchConfigs(BaseModel):
    mapper_config: Optional[ExecutePlugin]=None
    data_configs: List[ExecuteDataSource]
    assembly_config: Optional[ExecutePlugin]=None
    filter_configs: List[ExecutePlugin]
    score_config: ExecutePlugin
    embedding_configs: List[ExecutePlugin]
    embedding_dict: Optional[Dict[int, ExecutePlugin]]=None # embedding lookup by id
    source_embeddings: Optional[Dict[int, List[ExecutePlugin]]]=None # assembly index to source embeddings
        
    def iter_plugins(self):
        for key in self.model_fields.keys():
            config = getattr(self, key)
            if (config is None) or (type(config) == dict):
                continue
            config = config if type(config) == list else [config]
            for c in config:
                yield c
                
    def update_embedding_dict(self, embedding_dict: dict):
        self.embedding_dict = {i.plugin_id : i for i in self.embedding_configs}

        for embedding_id, embedding in embedding_dict.items():
            if embedding_id in self.embedding_dict:
                continue 

            embedding_config = ExecutePlugin(plugin_id=embedding.id)
            embedding_config.plugin = embedding
            self.embedding_dict[embedding.id] = embedding_config
            
    def update_source_embeddings(self):
        self.source_embeddings = {}
        if self.mapper_config is not None:
            mapper_plugin = self.mapper_config.plugin
            source_embedding_id = mapper_plugin.input_embedding_id
            self.source_embeddings[0] = [self.embedding_dict[source_embedding_id]]
        else:
            for data_config in self.data_configs:
                embedding_ids = data_config.plugin.embedding_ids
                assembly_index = data_config.data_source_params.assembly_index
                data_embeddings = []
                for source_embedding_id in data_config.plugin.embedding_ids:
                    data_embeddings.append(self.embedding_dict[source_embedding_id])
                self.source_embeddings[assembly_index] = data_embeddings
                
    def update_data_configs(self):
        data_configs = self.data_configs
        assembly_config = self.assembly_config
        if assembly_config is None:
            return 
        
        n_parents = assembly_config.plugin.num_parents
        if len(data_configs) == 1:
            new_data_configs = []
            for i in range(n_parents):
                data_config = data_configs[0].model_dump()
                data_config['data_source_params']['assembly_index'] = i
                new_data_configs.append(ExecuteDataSource(**data_config))
                
            self.data_configs = new_data_configs
        
    @classmethod
    def from_create_config(cls, create_config: HCCreateConfigs):
        create_dict = {}
        for key in create_config.model_fields.keys():
            config = getattr(create_config, key)
            if key=='data_config':
                key = 'data_configs'
                config = [config]
            
            if config is None:
                if key in ['filter_configs', 'embedding_configs']:
                    config = []
                create_dict[key] = config
            elif type(config) == list:
                if key =='data_configs':
                    create_dict[key] = [ExecuteDataSource(**i.model_dump()) for i in config]
                else:
                    create_dict[key] = [ExecutePlugin(**i.model_dump()) for i in config]
            else:
                create_dict[key] = ExecutePlugin(**config.model_dump())
        return cls(**create_dict)

class HCSearchIteration(BaseModel):
    update_params: HCAssembledUpdateParams
    query_embeddings: List[GradientEmbedding]
    query: Optional[Query]=None
    results: Optional[List[InternalItem]]=None
        
    def set_query(self):
        embeddings = [q.get_embeddings() for q in self.query_embeddings]
        if len(embeddings) == 1:
            queries = [QueryEmbedding(query_group=i, embedding=e.embedding, assembled_embeddings=None)
                       for i, e in enumerate(embeddings[0])]
        else:
            queries = [QueryEmbedding(query_group=i, embedding=None, assembled_embeddings=e)
                       for i, e in enumerate(zip(*embeddings))]
        self.query = Query(queries=queries)
        
    def get_results(self, query_group=None, deduplicate=False, only_valid=False):
        results = self.results
        if only_valid:
            results = [i for i in results if i.valid]
        
        if query_group is not None:
            results = [i for i in results if i.query_group == query_group]
            
        if deduplicate:
            unique_results = {i.item_data.item_id:i for i in results}
            results = list(unique_results.values())
            
        return results 

