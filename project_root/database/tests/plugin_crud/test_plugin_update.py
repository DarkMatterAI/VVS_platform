import pytest
from vvs_database import schemas

@pytest.mark.asyncio
async def test_update_embedding_plugin(db_session, create_test_embedding):
    """Test updating an embedding plugin"""
    # Create a plugin to update
    plugin = await create_test_embedding()
    
    # Update data
    update_data = schemas.PluginUpdate(
        name="Updated Embedding",
        timeout=60,
        vector_length=256,
        distance_metric=schemas.DistanceMetric.Euclid
    )
    
    # Update the plugin
    from vvs_database.crud import update_plugin
    updated_plugin = await update_plugin(db_session, plugin.id, update_data)
    
    # Assert the plugin was updated
    assert updated_plugin.id == plugin.id
    assert updated_plugin.name == "Updated Embedding"
    assert updated_plugin.timeout == 60
    assert updated_plugin.vector_length == 256
    assert updated_plugin.distance_metric == schemas.DistanceMetric.Euclid
    await db_session.commit()

@pytest.mark.asyncio
async def test_update_data_source_plugin_embeddings(db_session, create_test_embedding):
    """Test updating a data source plugin's embedding relationships"""
    # Create embeddings
    embedding1 = await create_test_embedding(name="Embedding 1")
    embedding2 = await create_test_embedding(name="Embedding 2")
    
    # Create a data source plugin with embedding1
    from vvs_database.crud import create_plugin
    plugin_data = schemas.DataSourcePluginCreate(
        name="Test Data Source",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.DATA_SOURCE,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=1,
        embedding_ids=[embedding1.id]
    )
    plugin = await create_plugin(db_session, plugin_data)
    
    # Update to use embedding2
    update_data = schemas.PluginUpdate(
        embedding_ids=[embedding2.id]
    )
    
    # Update the plugin
    from vvs_database.crud import update_plugin
    updated_plugin = await update_plugin(db_session, plugin.id, update_data, response_model=True)
    
    # Assert relationships were updated
    assert updated_plugin.embedding_ids == [embedding2.id], updated_plugin
    
    # Now update to use both embeddings
    update_data = schemas.PluginUpdate(
        embedding_ids=[embedding1.id, embedding2.id]
    )
    updated_plugin = await update_plugin(db_session, plugin.id, update_data, response_model=True)
    
    # Assert relationships were updated
    assert set(updated_plugin.embedding_ids) == {embedding1.id, embedding2.id}
    await db_session.commit()

@pytest.mark.asyncio
async def test_update_mapper_plugin(db_session, create_test_embedding):
    """Test updating a mapper plugin's relationships"""
    # Create embeddings
    input_embedding = await create_test_embedding(name="Input Embedding")
    output_embedding1 = await create_test_embedding(name="Output Embedding 1")
    output_embedding2 = await create_test_embedding(name="Output Embedding 2")
    
    # Create mapper plugin
    from vvs_database.crud import create_plugin
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
        batch_size=1,
        input_embedding_id=input_embedding.id,
        output_order=output_order
    )
    plugin = await create_plugin(db_session, plugin_data)
    
    # Create new embeddings for update
    new_input = await create_test_embedding(name="New Input Embedding")
    new_output = await create_test_embedding(name="New Output Embedding")
    
    # Update the input embedding
    update_data = schemas.PluginUpdate(
        input_embedding_id=new_input.id
    )
    
    # Update the plugin
    from vvs_database.crud import update_plugin
    updated_plugin = await update_plugin(db_session, plugin.id, update_data, response_model=True)
    
    # Assert input embedding was updated
    assert updated_plugin.input_embedding_id == new_input.id
    
    # Now update the output order
    new_output_order = [
        schemas.OutputEmbedding(index=0, embedding_id=output_embedding1.id),
        schemas.OutputEmbedding(index=1, embedding_id=new_output.id)
    ]
    
    update_data = schemas.PluginUpdate(
        output_order=new_output_order
    )
    
    updated_plugin = await update_plugin(db_session, plugin.id, update_data, response_model=True)
    
    # Assert output order was updated
    assert updated_plugin.output_order[0].embedding_id == output_embedding1.id
    assert updated_plugin.output_order[1].embedding_id == new_output.id
    
    # Check embedding relationships 
    assert new_input.id in updated_plugin.embedding_ids
    assert output_embedding1.id in updated_plugin.embedding_ids
    assert new_output.id in updated_plugin.embedding_ids
    await db_session.commit()

@pytest.mark.asyncio
async def test_update_assembly_plugin(db_session):
    """Test updating an assembly plugin"""
    # Create an assembly plugin
    from vvs_database.crud import create_plugin
    plugin_data = schemas.AssemblyPluginCreate(
        name="Test Assembly",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.ASSEMBLY,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1,
        batch_size=1,
        num_parents=2
    )
    plugin = await create_plugin(db_session, plugin_data)
    
    # Update data
    update_data = schemas.PluginUpdate(
        name="Updated Assembly",
        num_parents=3
    )
    
    # Update the plugin
    from vvs_database.crud import update_plugin
    updated_plugin = await update_plugin(db_session, plugin.id, update_data)
    
    # Assert the plugin was updated
    assert updated_plugin.id == plugin.id
    assert updated_plugin.name == "Updated Assembly"
    assert updated_plugin.num_parents == 3
    await db_session.commit()