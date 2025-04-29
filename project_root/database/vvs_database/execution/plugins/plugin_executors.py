from typing import List, Any, Dict
from vvs_database.schemas import (
    PluginType,
    ItemRequest,
    EmbedResponse,
    FilterResponse,
    ScoreResponse,
    DataSourceRequest,
    DataSourceResponse,
    MapperRequest, 
    MapperResponse,
    AssemblyRequest, 
    AssemblyResponse
)

from vvs_database.schemas import (
    ExecuteRequestUnion, 
    ExecuteResponseUnion, 
    ExecuteParams,
    DataSourceRequest,
    DataSourceResponse,
    PluginInDB
)
from vvs_database.execution.plugins.base_executor import BasePluginExecutor
from vvs_database import logging 

class ItemPluginExecutor(BasePluginExecutor):
    async def check_in_results(self, 
                               requests: List[ItemRequest], 
                               results: List[EmbedResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in embedding results to database"""
        if self.execute_params.db_persist:
            return await self.connections.db_service.check_in_item_results(
            self.plugin, requests, results, valid_execution
        )
        return None 

    async def query_database(self, 
                             plugin: PluginInDB, 
                             requests: Dict[str, ExecuteRequestUnion]
                             ) -> Dict[str, ExecuteResponseUnion]:
        logging.info(f"{self.log_id}: Looking up DB results for {len(requests.keys())} requests")
        result = {}
        if self.execute_params.db_lookup:
            result = await self.connections.db_service.get_item_results(plugin, requests)
            result = {k: self.response_model.model_validate(v) for k, v in result.items()}
        return result 

class EmbeddingPluginExecutor(ItemPluginExecutor):
    """Executor for embedding plugins"""
    
    request_model = ItemRequest
    response_model = EmbedResponse
    
class DataSourcePluginExecutor(BasePluginExecutor):
    """Executor for data source plugins"""
    
    request_model = DataSourceRequest
    response_model = DataSourceResponse

    def update_params(self, execute_params: ExecuteParams):
        """
        We do not generate unique keys from embeddings, so data source 
        requests are not elegible to be cached / saved, so we set `cache` 
        and `db_lookup` to False
        """
        execute_params.cache = False
        execute_params.db_lookup = False 
        return execute_params 
    
    async def check_in_results(self, 
                               requests: List[DataSourceRequest], 
                               results: List[DataSourceResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """
        We check in the items in the data query result. If `db_persist`, 
        we also check in embeddings from the results
        """
        return await self.connections.db_service.check_in_data_source_results(
            self.plugin, requests, results, valid_execution, self.execute_params.db_persist
        )

class FilterPluginExecutor(ItemPluginExecutor):
    """Executor for filter plugins"""
    
    request_model = ItemRequest
    response_model = FilterResponse

class ScorePluginExecutor(ItemPluginExecutor):
    """Executor for score plugins"""
    
    request_model = ItemRequest
    response_model = ScoreResponse

    def update_params(self, execute_params: ExecuteParams):
        """
        Score persistence / logging is required for tracking scored 
        results - set `db_persist` and `log_execute_keys` to True
        """
        execute_params.db_persist = True 
        execute_params.log_execute_keys = True 
        return execute_params 

class MapperPluginExecutor(BasePluginExecutor):
    """Executor for mapper plugins"""
    
    request_model = MapperRequest
    response_model = MapperResponse

    def update_params(self, execute_params: ExecuteParams):
        """
        We do not generate unique keys from embeddings, so mapper 
        requests are not elegible to be cached / saved, so we set `cache` 
        and `db_lookup` to False. Similarly, mapper results (embeddings) 
        are elegible to be saved (`db_persist` False)
        """
        execute_params.cache = False
        execute_params.db_lookup = False 
        execute_params.db_persist = False  
        return execute_params 

class AssemblyPluginExecutor(BasePluginExecutor):
    """Executor for assembly plugins"""
    
    request_model = AssemblyRequest
    response_model = AssemblyResponse
    
    async def check_in_results(self, 
                               requests: List[AssemblyRequest], 
                               results: List[AssemblyResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """
        Assembly results are required for related item records, so 
        we always check in 
        """
        return await self.connections.db_service.check_in_assembly_results(
            self.plugin, requests, results, valid_execution
        )
    
    async def query_database(self, 
                             plugin: PluginInDB, 
                             requests: Dict[str, ExecuteRequestUnion]
                             ) -> Dict[str, ExecuteResponseUnion]:
        logging.info(f"{self.log_id}: Looking up DB results for {len(requests.keys())} requests")
        result = {}
        if self.execute_params.db_lookup:
            result = await self.connections.db_service.get_assembly_results(plugin, requests)
            result = {k: self.response_model.model_validate(v) for k, v in result.items()}
        return result 


EXECUTOR_DICT = {
    PluginType.EMBEDDING : EmbeddingPluginExecutor,
    PluginType.FILTER : FilterPluginExecutor,
    PluginType.SCORE : ScorePluginExecutor,
    PluginType.DATA_SOURCE : DataSourcePluginExecutor,
    PluginType.MAPPER : MapperPluginExecutor,
    PluginType.ASSEMBLY : AssemblyPluginExecutor
}
