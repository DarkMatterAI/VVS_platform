from typing import List, Any
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
from vvs_database.schemas import DataSourceRequest, DataSourceResponse
from vvs_database.execution.plugins.base_executor import BasePluginExecutor

class EmbeddingPluginExecutor(BasePluginExecutor):
    """Executor for embedding plugins"""
    
    request_model = ItemRequest
    response_model = EmbedResponse
    
    async def check_in_results(self, 
                               requests: List[ItemRequest], 
                               results: List[EmbedResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in embedding results to database"""
        if self.db_persist:
            return await self.db_service.check_in_item_results(
            self.plugin, requests, results, valid_execution
        )
        return None 

class DataSourcePluginExecutor(BasePluginExecutor):
    """Executor for data source plugins"""
    
    request_model = DataSourceRequest
    response_model = DataSourceResponse
    
    async def check_in_results(self, 
                               requests: List[DataSourceRequest], 
                               results: List[DataSourceResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in data source results to database"""
        return await self.db_service.check_in_data_source_results(
            self.plugin, requests, results, valid_execution, self.db_persist
        )

class FilterPluginExecutor(BasePluginExecutor):
    """Executor for filter plugins"""
    
    request_model = ItemRequest
    response_model = FilterResponse
    
    async def check_in_results(self, 
                               requests: List[ItemRequest], 
                               results: List[FilterResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in filter results to database"""
        if self.db_persist:
            return await self.db_service.check_in_item_results(
                self.plugin, requests, results, valid_execution
            )
        return None 

class ScorePluginExecutor(BasePluginExecutor):
    """Executor for score plugins"""
    
    request_model = ItemRequest
    response_model = ScoreResponse
    
    async def check_in_results(self, 
                               requests: List[ItemRequest], 
                               results: List[ScoreResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in score results to database"""
        # score always checks in, regardless of db_persist
        return await self.db_service.check_in_item_results(
            self.plugin, requests, results, valid_execution
        )

class MapperPluginExecutor(BasePluginExecutor):
    """Executor for mapper plugins"""
    
    request_model = MapperRequest
    response_model = MapperResponse
    
    async def check_in_results(self, 
                               requests: List[MapperRequest], 
                               results: List[MapperResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in mapper results to database - mapper doesn't save results"""
        return None

class AssemblyPluginExecutor(BasePluginExecutor):
    """Executor for assembly plugins"""
    
    request_model = AssemblyRequest
    response_model = AssemblyResponse
    
    async def check_in_results(self, 
                               requests: List[AssemblyRequest], 
                               results: List[AssemblyResponse],
                               valid_execution: List[bool],
                               ) -> Any:
        """Check in assembly results to database"""
        # always check in assembly
        return await self.db_service.check_in_assembly_results(
            self.plugin, requests, results, valid_execution
        )

EXECUTOR_DICT = {
    PluginType.EMBEDDING : EmbeddingPluginExecutor,
    PluginType.FILTER : FilterPluginExecutor,
    PluginType.SCORE : ScorePluginExecutor,
    PluginType.DATA_SOURCE : DataSourcePluginExecutor,
    PluginType.MAPPER : MapperPluginExecutor,
    PluginType.ASSEMBLY : AssemblyPluginExecutor
}
