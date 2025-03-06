import pytest
import pydantic 
from vvs_database import schemas, crud 
from vvs_database.exceptions import ValidationError

@pytest.mark.asyncio
async def test_create_embedding_plugin(db_session):
    """Test creating an embedding plugin"""
    plugin_data = schemas.EmbeddingPluginCreate(
        name="Test Embedding",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.EMBEDDING,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=10,
        vector_length=128,
        distance_metric=schemas.DistanceMetric.Cosine
    )
    
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=False)
    
    assert plugin.id is not None
    assert plugin.name == "Test Embedding"
    assert plugin.type == schemas.PluginType.EMBEDDING
    assert plugin.plugin_class == schemas.PluginClass.GENERIC
    assert plugin.execution_type == schemas.PluginExecutionType.QUEUE
    assert plugin.group_key == "test_group"
    assert plugin.vector_length == 128
    assert plugin.distance_metric == schemas.DistanceMetric.Cosine

@pytest.mark.asyncio
async def test_create_data_source_plugin(db_session, create_test_embedding):
    """Test creating a data source plugin"""
    embedding = await create_test_embedding()
    
    plugin_data = schemas.DataSourcePluginCreate(
        name="Test Data Source",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.DATA_SOURCE,
        execution_type=schemas.PluginExecutionType.API,
        endpoint_url="http://test-endpoint/execute",
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=10,
        embedding_ids=[embedding.id]
    )
    
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=True)
    
    assert plugin.id is not None
    assert plugin.name == "Test Data Source"
    assert plugin.type == schemas.PluginType.DATA_SOURCE
    assert plugin.execution_type == schemas.PluginExecutionType.API
    assert plugin.endpoint_url == "http://test-endpoint/execute"
    assert plugin.embedding_ids == [embedding.id]


@pytest.mark.asyncio
async def test_create_filter_plugin(db_session, create_test_embedding):
    """Test creating a filter plugin"""
    plugin_data = schemas.FilterPluginCreate(
        name="Test Filter",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.FILTER,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=10
    )
    
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=True)
    
    assert plugin.id is not None
    assert plugin.name == "Test Filter"
    assert plugin.type == schemas.PluginType.FILTER
    assert plugin.embedding_ids is None 
    
    embedding = await create_test_embedding()
    plugin_data.embedding_ids = [embedding.id]
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=True)
    
    assert plugin.embedding_ids == [embedding.id]

@pytest.mark.asyncio
async def test_create_score_plugin(db_session, create_test_embedding):
    """Test creating a score plugin"""
    embedding = await create_test_embedding()
    
    plugin_data = schemas.ScorePluginCreate(
        name="Test Score",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.SCORE,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=10,
        embedding_ids=[embedding.id]
    )
    
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=True)
    
    assert plugin.id is not None
    assert plugin.name == "Test Score"
    assert plugin.type == schemas.PluginType.SCORE
    assert plugin.embedding_ids == [embedding.id]

@pytest.mark.asyncio
async def test_create_mapper_plugin(db_session, create_test_embedding):
    """Test creating a mapper plugin"""
    input_embedding = await create_test_embedding(name="Input Embedding")
    output_embedding1 = await create_test_embedding(name="Output Embedding 1")
    output_embedding2 = await create_test_embedding(name="Output Embedding 2")
    
    output_order = [
        schemas.OutputEmbedding(index=0, embedding_id=output_embedding1.id),
        schemas.OutputEmbedding(index=1, embedding_id=output_embedding2.id)
    ]
    
    plugin_data = schemas.MapperPluginCreate(
        name="Test Mapper",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.MAPPER,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=10,
        input_embedding_id=input_embedding.id,
        output_order=output_order
    )
    
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=True)
    
    assert plugin.id is not None
    assert plugin.name == "Test Mapper"
    assert plugin.type == schemas.PluginType.MAPPER
    assert plugin.input_embedding_id == input_embedding.id
    assert len(plugin.output_order) == 2
    assert plugin.output_order[0].embedding_id == output_embedding1.id
    assert plugin.output_order[1].embedding_id == output_embedding2.id
    
    embedding_ids = set(plugin.embedding_ids)
    assert len(embedding_ids) == 3
    assert input_embedding.id in embedding_ids
    assert output_embedding1.id in embedding_ids
    assert output_embedding2.id in embedding_ids

@pytest.mark.asyncio
async def test_create_assembly_plugin(db_session):
    """Test creating an assembly plugin"""
    plugin_data = schemas.AssemblyPluginCreate(
        name="Test Assembly",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.ASSEMBLY,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=10,
        num_parents=2
    )
    
    plugin = await crud.create_plugin(db_session, plugin_data, response_model=False)
    
    assert plugin.id is not None
    assert plugin.name == "Test Assembly"
    assert plugin.type == schemas.PluginType.ASSEMBLY
    assert plugin.num_parents == 2

@pytest.mark.asyncio
async def test_create_plugin_with_invalid_data(db_session):
    """Test creating a plugin with invalid data"""
    
    with pytest.raises(pydantic.ValidationError):
        plugin_data = schemas.EmbeddingPluginCreate(
            name="Invalid Embedding",
            plugin_class=schemas.PluginClass.GENERIC,
            type=schemas.PluginType.EMBEDDING,
            execution_type=schemas.PluginExecutionType.QUEUE,
            # Missing group_key, timeout, etc.
            vector_length=128,
            distance_metric=schemas.DistanceMetric.Cosine
        )
        plugin = await crud.create_plugin(db_session, plugin_data, response_model=False)
    
    # API execution_type requires endpoint_url
    with pytest.raises(pydantic.ValidationError):
        plugin_data = schemas.EmbeddingPluginCreate(
            name="Invalid Embedding",
            plugin_class=schemas.PluginClass.GENERIC,
            type=schemas.PluginType.EMBEDDING,
            execution_type=schemas.PluginExecutionType.API,
            group_key="test_group",
            timeout=30,
            max_concurrency=5,
            max_retries=1,
            # Missing endpoint_url
            vector_length=128,
            distance_metric=schemas.DistanceMetric.Cosine
        )
        plugin = await crud.create_plugin(db_session, plugin_data, response_model=False)

