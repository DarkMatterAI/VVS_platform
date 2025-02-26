import pytest
from vvs_database import schemas

@pytest.mark.asyncio
async def test_get_plugins_summary(db_session, create_test_embedding):
    """Test getting a summary of plugins by type"""
    # Create one of each plugin type
    embedding = await create_test_embedding()
    
    from vvs_database.crud import create_plugin
    
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
    await create_plugin(db_session, data_source)
    
    filter_plugin = schemas.FilterPluginCreate(
        name="Filter Plugin",
        plugin_class=schemas.PluginClass.GENERIC,
        type=schemas.PluginType.FILTER,
        execution_type=schemas.PluginExecutionType.QUEUE,
        group_key="test_group",
        timeout=30,
        max_concurrency=5,
        max_retries=1
    )
    await create_plugin(db_session, filter_plugin)
    
    # Get the summary
    from vvs_database.crud import get_plugins_summary
    summary = await get_plugins_summary(db_session)
    
    # Assert the summary contains counts for all plugin types
    assert summary["embedding"] >= 1
    assert summary["data_source"] >= 1
    assert summary["filter"] >= 1
    assert summary["total"] >= 3
    await db_session.commit()

@pytest.mark.asyncio
async def test_count_plugins_by_class(db_session, create_test_embedding):
    """Test counting plugins by class"""
    # Create plugins of different classes
    await create_test_embedding(plugin_class=schemas.PluginClass.GENERIC)
    await create_test_embedding(plugin_class=schemas.PluginClass.INTERNAL_RDKIT)
    await create_test_embedding(plugin_class=schemas.PluginClass.INTERNAL_RDKIT)
    
    # Count by class
    from vvs_database.crud import count_plugins_by_class
    
    generic_count = await count_plugins_by_class(db_session, schemas.PluginClass.GENERIC)
    rdkit_count = await count_plugins_by_class(db_session, schemas.PluginClass.INTERNAL_RDKIT)
    
    # Assert the counts are correct
    assert generic_count >= 1
    assert rdkit_count >= 2
    await db_session.commit()