import pytest
from vvs_database import schemas, crud 
from vvs_database.exceptions import ReferenceError

@pytest.mark.asyncio
async def test_delete_plugin(db_session, get_plugin, create_test_embedding):
    """Test deleting a plugin"""
    plugin = await create_test_embedding()
    
    deleted_plugin = await crud.delete_plugin(db_session, plugin.id)
    
    assert deleted_plugin.id == plugin.id
    
    plugin = await get_plugin(plugin.id, with_error=False)
    assert plugin is None

@pytest.mark.asyncio
async def test_cannot_delete_embedding_with_references(db_session, create_test_embedding):
    """Test that an embedding plugin cannot be deleted if other plugins reference it"""
    # Create an embedding plugin
    embedding1 = await create_test_embedding(name="Referenced Embedding")
    embedding2 = await create_test_embedding(name="Referenced Embedding")
    
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
        embedding_ids=[embedding1.id]
    )
    await crud.create_plugin(db_session, data_source)
    
    # Try to delete the embedding
    with pytest.raises(ReferenceError):
        await crud.delete_plugin(db_session, embedding1.id)
    
    # Create a mapper plugin that references the embedding
    mapper = schemas.MapperPluginCreate(
        name="Mapper",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.MAPPER,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        input_embedding_id=embedding1.id,
        output_order=[
            schemas.OutputEmbedding(index=0, embedding_id=embedding2.id),
            schemas.OutputEmbedding(index=1, embedding_id=embedding2.id)
        ]
    )
    
    # Clean up the data source first
    mapper_plugin = await crud.create_plugin(db_session, mapper)
    await crud.delete_plugin(db_session, mapper_plugin.id)
    
    # You should still not be able to delete the embedding
    with pytest.raises(ReferenceError):
        await crud.delete_plugin(db_session, embedding1.id)

    await db_session.commit()