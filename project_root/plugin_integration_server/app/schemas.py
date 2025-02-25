from typing import Union, List 
from vvs_database.schemas import (
    EmbedRequest, 
    EmbedResponse,
    DataSourceRequest,
    DataSourceResponse,
    MapperRequest,
    MapperResponse
)


EmbedRequestUnion = Union[EmbedRequest, List[EmbedRequest]]
EmbedResponseUnion = Union[EmbedResponse, List[EmbedResponse]]

DataSourceRequestUnion = Union[DataSourceRequest, List[DataSourceRequest]]
DataSourceResponseUnion = Union[DataSourceResponse, List[DataSourceResponse]]

MapperRequestUnion = Union[MapperRequest, List[MapperRequest]]
MapperResponseUnion = Union[MapperResponse, List[MapperResponse]]
