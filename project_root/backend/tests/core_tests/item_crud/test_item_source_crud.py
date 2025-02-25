import pytest
import sqlalchemy

plugin_api_str = '/api/v1/plugins'

@pytest.mark.asyncio
async def test_item_source_create(client, create_item, create_test_embedding, create_item_source):
    item = await create_item()
    plugin = await create_test_embedding()
    item_source = await create_item_source(item.id, plugin.id, external_id='test')
    assert item_source.external_id == 'test'
    assert item_source.item_id == item.id
    assert item_source.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_source_create_fails_invalid_ids(client, create_item_source):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        item_source = await create_item_source(10000000, 20000000, external_id='test')

@pytest.mark.asyncio
async def test_item_source_get(client, get_item_source, create_item_plugin_source):
    item_source = await get_item_source(999999999, 9999999)
    assert item_source is None 

    external_id = 'test'
    item, plugin, item_source = await create_item_plugin_source(external_id=external_id)

    item_source_get = await get_item_source(item.id, plugin.id)
    assert item_source_get is not None 
    assert item_source_get.external_id==external_id

@pytest.mark.asyncio
async def test_item_source_delete(client, create_item_plugin_source, get_item_source, delete_item_source):
    item, plugin, item_source = await create_item_plugin_source()

    _ = await delete_item_source(item_source)

    item_source_get = await get_item_source(item.id, plugin.id)
    assert item_source_get is None 

@pytest.mark.asyncio
async def test_cleanup_items(client, create_item_plugin_source, create_item, 
                             cleanup_items, get_item, get_item_source):
    item1, plugin1, item_source1 = await create_item_plugin_source()
    item2 = await create_item()

    deleted_count = await cleanup_items()
    assert deleted_count > 0

    item1_get = await get_item(item1.id)
    assert item1_get is not None 

    item_source1_get = await get_item_source(item1.id, plugin1.id)
    assert item_source1 is not None 

    item2_get = await get_item(item2.id)
    assert item2_get is None 


@pytest.mark.asyncio
async def test_item_checkin(client, item_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

@pytest.mark.asyncio
async def test_item_checkin_duplicates(client, item_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": "bulk item 1", "external_id": "ext1"},
    ]

    results = await item_checkin(items_data, plugin.id)

@pytest.mark.asyncio
async def test_item_checkin_conflict(client, item_checkin, create_item_plugin_source):
    item, plugin, item_source = await create_item_plugin_source()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": item.item, "external_id": item_source.external_id},
    ]

    results = await item_checkin(items_data, plugin.id)

@pytest.mark.asyncio
async def test_item_delete_source_propagation(client, create_item_plugin_source, delete_item,
                                              get_item_source):
    item, plugin, item_source = await create_item_plugin_source()

    # delete item
    response = await delete_item(item)

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check plugin still exists
    response = await client.get(f"{plugin_api_str}/{plugin.id}")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_plugin_delete_source_propagation(client, create_item_plugin_source,
                                              get_item_source, get_item, cleanup_items):
    item, plugin, item_source = await create_item_plugin_source()

    # delete plugin
    response = await client.delete(f"{plugin_api_str}/{plugin.id}")
    assert response.status_code == 200

    # check plugin deleted 
    response = await client.get(f"{plugin_api_str}/{plugin.id}")
    assert response.status_code == 404

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check item still exists
    item_record = await get_item(item.id)
    assert item_record is not None 

    # run cleanup
    deleted_count = await cleanup_items()
    assert deleted_count > 0

    # check item deleted
    item_record = await get_item(item.id)
    assert item_record is None 

