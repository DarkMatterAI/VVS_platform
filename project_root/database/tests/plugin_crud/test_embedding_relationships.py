import pytest
from vvs_database import schemas, crud

@pytest.mark.asyncio
async def test_validate_embedding_ids(db_session, create_test_embedding):
    """Test the validate_embedding_ids function"""
    # Create some embedding plugins
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    
    # Valid case - both embeddings exist
    await crud.validate_embedding_ids(db_session, [embedding1.id, embedding2.id])
    
    # Invalid case - nonexistent embedding ID
    from vvs_database.exceptions import ValidationError
    with pytest.raises(ValidationError):
        await crud.validate_embedding_ids(db_session, [embedding1.id, 99999])
    
    # Invalid case - duplicate embedding IDs
    with pytest.raises(ValidationError):
        await crud.validate_embedding_ids(db_session, [embedding1.id, embedding1.id])

    await db_session.commit()

@pytest.mark.asyncio
async def test_count_plugins_linked_to_embedding_id(db_session, create_test_embedding):
    """Test counting plugins linked to a specific embedding ID"""
    # Create an embedding plugin
    embedding = await create_test_embedding()
    
    # Create a data source plugin that references the embedding
    data_source = schemas.DataSourcePluginCreate(
        name="Data Source",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.DATA_SOURCE,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        embedding_ids=[embedding.id]
    )
    await crud.create_plugin(db_session, data_source)
    
    # Count plugins linked to the embedding
    count = await crud.count_plugins_linked_to_embedding_id(db_session, embedding.id)
    
    # Assert the count is correct
    assert count == 1
    
    # Create another plugin linked to the embedding
    filter_plugin = schemas.FilterPluginCreate(
        name="Filter Plugin",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.FILTER,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        embedding_ids=[embedding.id]
    )
    await crud.create_plugin(db_session, filter_plugin)
    
    # Count again
    count = await crud.count_plugins_linked_to_embedding_id(db_session, embedding.id)
    
    # Assert the count has increased
    assert count == 2

    await db_session.commit()
