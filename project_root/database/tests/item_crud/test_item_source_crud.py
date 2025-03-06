import pytest
import sqlalchemy

from vvs_database import crud 

@pytest.mark.asyncio
async def test_item_source_create(create_item, create_test_embedding, create_item_source):
    item = await create_item()
    plugin = await create_test_embedding()
    item_source = await create_item_source(item.id, plugin.id, external_id='test')
    assert item_source.external_id == 'test'
    assert item_source.item_id == item.id
    assert item_source.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_source_create_fails_invalid_ids(create_item_source):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        item_source = await create_item_source(10000000, 20000000, external_id='test')

@pytest.mark.asyncio
async def test_item_source_get(get_item_source, create_item_plugin_source):
    item_source = await get_item_source(999999999, 9999999)
    assert item_source is None 

    external_id = 'test'
    item, plugin, item_source = await create_item_plugin_source(external_id=external_id)

    item_source_get = await get_item_source(item.id, plugin.id)
    assert item_source_get is not None 
    assert item_source_get.external_id==external_id

@pytest.mark.asyncio
async def test_get_item_sources(get_item_sources, create_item, 
                                create_test_embedding, create_item_source):
    plugin = await create_test_embedding()
    n_items = 3

    item_ids = {}
    for i in range(n_items):
        item = await create_item()
        external_id = f"test_get_item_results_{i}"
        item_source = await create_item_source(item.id, plugin.id, external_id)
        item_ids[item.id] = external_id

    item_sources_get = await get_item_sources(item_ids, plugin.id)
    assert len(item_sources_get) == n_items

    for source in item_sources_get:
        assert source.plugin_id == plugin.id
        assert source.item_id in item_ids 
        assert source.external_id == item_ids[source.item_id]

@pytest.mark.asyncio
async def test_item_source_delete(db_session, 
                                  create_item_plugin_source, 
                                  get_item_source):
    item, plugin, item_source = await create_item_plugin_source()

    result = await crud.delete_item_source(db_session, item_source)

    item_source_get = await get_item_source(item.id, plugin.id)
    assert item_source_get is None 

@pytest.mark.asyncio
async def test_cleanup_items(db_session,
                             create_item_plugin_source, 
                             create_item, 
                             get_item, 
                             get_item_source):
    item1, plugin1, item_source1 = await create_item_plugin_source()
    item2 = await create_item()
    deleted_count = await crud.cleanup_unreferenced_items(db_session)
    
    assert deleted_count > 0

    item1_get = await get_item(item1.id)
    assert item1_get is not None 

    item_source1_get = await get_item_source(item1.id, plugin1.id)
    assert item_source1 is not None 

    item2_get = await get_item(item2.id)
    assert item2_get is None 

@pytest.mark.asyncio
async def test_item_delete_source_propagation(db_session, 
                                              create_item_plugin_source,
                                              get_item_source, 
                                              get_plugin):
    item, plugin, item_source = await create_item_plugin_source()

    # delete item
    _ = await crud.delete_item(db_session, item)

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check plugin still exists
    response = await get_plugin(plugin.id)
    assert response is not None 

@pytest.mark.asyncio
async def test_plugin_delete_source_propagation(db_session, 
                                                create_item_plugin_source,
                                                get_item_source, 
                                                get_item, 
                                                get_plugin):
    
    item, plugin, item_source = await create_item_plugin_source()

    # delete plugin
    result = await crud.delete_plugin(db_session, plugin.id)

    # check plugin deleted 
    response = await get_plugin(plugin.id, with_error=False)
    assert response is None 

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check item still exists
    item_record = await get_item(item.id)
    assert item_record is not None 

    # run cleanup
    deleted_count = await crud.cleanup_unreferenced_items(db_session)
    assert deleted_count > 0

    # check item deleted
    item_record = await get_item(item.id)
    assert item_record is None 

