from vvs_database.schemas.enums import (
    PluginType,
    PluginClass,
    PluginExecutionType,
    DistanceMetric,
    JobStatus,
    TERMINAL_STATUSES,
    JobType,
    ExecutionSources
)

from vvs_database.schemas.plugin_schemas import (
    PluginBase,
    PluginCreate,
    PluginUpdate,
    PluginInDB,
    EmbeddingPluginCreate,
    DataSourcePluginCreate,
    FilterPluginCreate,
    ScorePluginCreate,
    MapperPluginCreate,
    AssemblyPluginCreate,
    EmbeddingPluginInDB,
    DataSourcePluginInDB,
    FilterPluginInDB,
    ScorePluginInDB,
    MapperPluginInDB,
    AssemblyPluginInDB,
    PluginCreateUnion,
    PluginInDBUnion,
    OutputEmbedding
)

from vvs_database.schemas.item_schemas import (
    ItemBase,
    ItemCreate,
    ItemInDB,
    ItemSourceBase,
    ItemSourceCreate,
    ItemSourceInDB,
    ItemResultBase,
    ItemResultCreate,
    ItemResultInDB,
    NewItem,
    NewResult,
)

from vvs_database.schemas.execute_schemas import (
    RequestData,
    Embedding,
    ItemData,
    ItemDataEmbed,
    EmbedResponse,
    DataSourceRequest,
    DataSourceResponseItem,
    DataSourceResponse,
    ItemRequest,
    FilterResponse,
    ScoreResponse,
    MapperRequest,
    MapperResponse,
    AssemblyItem,
    AssemblyRequest,
    AssemblyResult,
    AssemblyResponse,
    ExecuteParams,
    ExecuteRequestUnion,
    BatchExecuteRequestUnion,
    ItemResponseUnion,
    ExecuteResponseUnion,
)

from vvs_database.schemas.assembly_schemas import (
    AssemblyComponentBase,
    AssemblyComponentCreate,
    AssemblyComponentInDB,
    AssemblyBase,
    AssemblyCreate,
    AssemblyInDB,
    AssemblyComponent,
    NewAssembly
)

# from vvs_database.schemas.connection_schemas import (
#     RabbitMQConnection,
#     RedisConnection,
#     PostgresConnection
# )

from vvs_database.schemas.job_schemas import (
    JobDBResponse,
    JobDBResponseUnion,
    UserItem,
    CreateQdrantUploadJob,
    QdrantUploadInternal,
)

from vvs_database.schemas.hc_schemas import (
    HCJobCreate,
    HCMapperJobCreate,
    HCAssembledJobCreate,
    HCUpdateParams,
    UpdateType,
    HCConfigCreate,
    HCAssembledConfigCreate,
    HCMapperConfigCreate,
    HCJobParams,
    HCInputItem,
)

from vvs_database.schemas.internal_schemas import (
    PluginOverrideParams,
    ExecutePlugin,
    ExecutePluginParams,
    ExecutePluginCreate,
    ExecuteDataSource,
    ExecuteDataParams,
    ExecuteDataSourceCreate,
    Parent,
    AssemblyItemInternal,
    InternalAssemblyData,
    InternalItem,
    ScoreResult,
    Query,
    QueryEmbedding,
    AssembledEmbedding,
)

__all__ = [
    # Enums
    "PluginType",
    "PluginClass",
    "PluginExecutionType",
    "DistanceMetric",
    "JobStatus",
    "TERMINAL_STATUSES",
    "JobType",
    "ExecutionSources",
    
    # Plugin schemas
    "PluginBase",
    "PluginCreate",
    "PluginUpdate",
    "PluginInDB",
    "EmbeddingPluginCreate",
    "DataSourcePluginCreate",
    "FilterPluginCreate",
    "ScorePluginCreate",
    "MapperPluginCreate",
    "AssemblyPluginCreate",
    "EmbeddingPluginInDB",
    "DataSourcePluginInDB",
    "FilterPluginInDB",
    "ScorePluginInDB",
    "MapperPluginInDB",
    "AssemblyPluginInDB",
    "PluginCreateUnion",
    "PluginInDBUnion",
    "OutputEmbedding",
    
    # Item schemas
    "ItemBase",
    "ItemCreate",
    "ItemInDB",
    "ItemSourceBase",
    "ItemSourceCreate",
    "ItemSourceInDB",
    "ItemResultBase",
    "ItemResultCreate",
    "ItemResultInDB",
    "NewItem",
    "NewResult",

    # assembly schemas
    "AssemblyComponentBase",
    "AssemblyComponentCreate",
    "AssemblyComponentInDB",
    "AssemblyBase",
    "AssemblyCreate",
    "AssemblyInDB",
    "AssemblyComponent",
    "NewAssembly",

    # Execute schemas
    "RequestData",
    "Embedding",
    "ItemData",
    "ItemDataEmbed",
    "EmbedResponse",
    "DataSourceRequest",
    "DataSourceResponseItem",
    "DataSourceResponse",
    "ItemRequest",
    "FilterResponse",
    "ScoreResponse",
    "MapperRequest",
    "MapperResponse",
    "AssemblyItem",
    "AssemblyRequest",
    "AssemblyResult",
    "AssemblyResponse",
    "ExecuteParams",
    "ExecuteRequestUnion",
    "BatchExecuteRequestUnion",
    "ItemResponseUnion",
    "ExecuteResponseUnion",

    # # connections
    # "RabbitMQConnection",
    # "RedisConnection",
    # "PostgresConnection",

    # job schemas
    "JobDBResponse",
    "JobDBResponseUnion",
    "UserItem",
    "CreateQdrantUploadJob",
    "QdrantUploadInternal",

    # hc job schemas
    "HCJobCreate",
    "HCMapperJobCreate",
    "HCAssembledJobCreate",
    "HCUpdateParams",
    "UpdateType",
    "HCConfigCreate",
    "HCAssembledConfigCreate",
    "HCMapperConfigCreate",
    "HCJobParams",
    "HCInputItem",

    # internal schemas
    "PluginOverrideParams",
    "ExecutePlugin",
    "ExecutePluginParams",
    "ExecutePluginCreate",
    "ExecuteDataSource",
    "ExecuteDataParams",
    "ExecuteDataSourceCreate",
    "Parent",
    "AssemblyItemInternal",
    "InternalAssemblyData",
    "InternalItem",
    "ScoreResult",
    "Query",
    "QueryEmbedding",
    "AssembledEmbedding",
]