import pytest
from vvs_database import schemas, crud 
from vvs_database.exceptions import NotFoundError

@pytest.mark.asyncio
async def test_get_plugin_by_id(db_session, create_test_embedding):
    """Test getting a plugin by ID"""
    # Create a test plugin
    plugin = await create_test_embedding()
    
    # Get the plugin by ID
    retrieved_plugin = await crud.get_plugin(db_session, plugin.id)
    
    # Assert the plugin was retrieved correctly
    assert retrieved_plugin.id == plugin.id
    assert retrieved_plugin.name == plugin.name
    assert retrieved_plugin.type == schemas.PluginType.EMBEDDING

    await db_session.commit()

@pytest.mark.asyncio
async def test_get_nonexistent_plugin(db_session):
    """Test getting a nonexistent plugin"""
    
    # Try to get a nonexistent plugin
    with pytest.raises(NotFoundError):
        await crud.get_plugin(db_session, 99999)
    
    # Try with with_error=False
    result = await crud.get_plugin(db_session, 99999, with_error=False)
    assert result is None
    await db_session.commit()

@pytest.mark.asyncio
async def test_get_plugins_with_filters(db_session, create_test_embedding):
    """Test getting plugins with filters"""
    # Create test plugins
    await create_test_embedding(name="Test Plugin 1", plugin_class=schemas.PluginClass.GENERIC)
    await create_test_embedding(name="Test Plugin 2", plugin_class=schemas.PluginClass.INTERNAL_RDKIT)
    await create_test_embedding(name="Other Plugin", plugin_class=schemas.PluginClass.GENERIC)
    
    # Get plugins with name filter
    plugins = await crud.get_plugins(db_session, filter_params={"name": "Test%"})
    
    # Assert the correct plugins were retrieved
    assert len(plugins) >= 2
    assert all("Test" in plugin.name for plugin in plugins)
    
    # Get plugins with plugin_class filter
    plugins = await crud.get_plugins(db_session, filter_params={"plugin_class": schemas.PluginClass.INTERNAL_RDKIT})
    
    # Assert the correct plugins were retrieved
    assert len(plugins) >= 1
    assert all(plugin.plugin_class == schemas.PluginClass.INTERNAL_RDKIT for plugin in plugins)
    
    # Get plugins with multiple filters
    plugins = await crud.get_plugins(
        db_session, 
        filter_params={
            "name": "Test%",
            "plugin_class": schemas.PluginClass.GENERIC
        }
    )
    
    # Assert the correct plugins were retrieved
    assert len(plugins) >= 1
    assert all("Test" in plugin.name for plugin in plugins)
    assert all(plugin.plugin_class == schemas.PluginClass.GENERIC for plugin in plugins)
    await db_session.commit()

@pytest.mark.asyncio
async def test_get_plugins_with_pagination(db_session, create_test_embedding):
    """Test getting plugins with pagination"""
    # Create multiple test plugins
    for i in range(5):
        await create_test_embedding(name=f"Pagination Test {i}")
    
    # Get plugins with pagination
    plugins_page1 = await crud.get_plugins(db_session, skip=0, limit=2)
    plugins_page2 = await crud.get_plugins(db_session, skip=2, limit=2)
    
    # Assert pagination works correctly
    assert len(plugins_page1) == 2
    assert len(plugins_page2) == 2
    assert plugins_page1[0].id != plugins_page2[0].id
    assert plugins_page1[1].id != plugins_page2[0].id
    await db_session.commit()

@pytest.mark.asyncio
async def test_get_plugins_with_response_model(db_session, create_test_embedding):
    """Test getting plugins with response_model=True"""
    # Create a test plugin
    plugin = await create_test_embedding()
    
    # Get the plugin with response_model=True
    plugin_response = await crud.get_plugin(db_session, plugin.id, response_model=True)
    
    # Assert that the returned object is a Pydantic model
    assert isinstance(plugin_response, schemas.EmbeddingPluginInDB)
    assert plugin_response.id == plugin.id
    assert plugin_response.name == plugin.name
    await db_session.commit()