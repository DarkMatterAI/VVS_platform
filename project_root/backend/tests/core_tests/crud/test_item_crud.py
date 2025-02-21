import pytest
import time 
from app import models

api_str = '/api/v1/item'
plugin_api_str = '/api/v1/plugins'

async def create_item_source(db_session, item, plugin, external_id=None):

    source = models.ItemSource(
        item_id=item.id,
        source_plugin_id=plugin.id,
        external_id=external_id
    )
    db_session.add(source)
    await db_session.commit()
    return source

@pytest.mark.asyncio
async def test_get_item_success(client, db_session):
    # First create an item in the database
    item_name = 'item_create_test_1'
    item = models.Item(item=item_name)
    db_session.add(item)
    await db_session.commit()
    
    response = await client.get(f"{api_str}/{item.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["item"] == item_name
    assert data["id"] == item.id
    assert "created_at" in data

@pytest.mark.asyncio
async def test_get_item_not_found(client):
    response = await client.get(f"{api_str}/99999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Item not found"

@pytest.mark.asyncio
async def test_delete_item_success(client, db_session):
    # Create an item to delete
    item_name = 'item_delete_test_1'
    item = models.Item(item=item_name)
    db_session.add(item)
    await db_session.commit()
    
    response = await client.delete(f"{api_str}/{item.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["item"] == item_name
    
    # Verify item is deleted
    response = await client.get(f"{api_str}/{item.id}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_delete_item_not_found(client):
    response = await client.delete(f"{api_str}/99999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Item not found"

@pytest.mark.asyncio
async def test_get_item_source_success(client, db_session, create_test_item, create_test_embedding):
    item = await create_test_item()
    plugin = await create_test_embedding()
    source = await create_item_source(db_session, item, plugin, "ext123")
    
    response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["item_id"] == item.id
    assert data["source_plugin_id"] == plugin.id
    assert data["external_id"] == "ext123"

@pytest.mark.asyncio
async def test_get_item_source_not_found(client):
    response = await client.get(f"{api_str}/999/sources/999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Item source not found"

@pytest.mark.asyncio
async def test_delete_item_source_success(client, db_session, create_test_item, create_test_embedding):
    item = await create_test_item()
    plugin = await create_test_embedding()
    source = await create_item_source(db_session, item, plugin, "ext123")
    
    response = await client.delete(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 200
    
    response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_cleanup_items(client, db_session, create_test_item, create_test_embedding):
    item1 = await create_test_item()
    item2 = await create_test_item()
    plugin = await create_test_embedding()
    source = await create_item_source(db_session, item1, plugin, "ext123")
    
    response = await client.post(f"{api_str}/cleanup")
    assert response.status_code == 200
    data = response.json()
    assert "deleted_count" in data
    assert data["deleted_count"] >= 1  # Should delete at least item2

    response = await client.get(f"{api_str}/{item1.id}") # item1 should still exist
    assert response.status_code == 200

    response = await client.get(f"{api_str}/{item1.id}/sources/{plugin.id}") # source should still exist
    assert response.status_code == 200

    response = await client.get(f"{api_str}/{item2.id}") # item2 should be deleted 
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_create_items_bulk_invalid_plugin(client):
    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"}
    ]
    
    response = await client.post(
        "/items/bulk?plugin_id=99999999",
        json=items_data
    )
    
    assert response.status_code == 404

async def bulk_create_helper(client, items_data, plugin):
    response = await client.post(
        f"{api_str}/bulk?plugin_id={plugin.id}",
        json=items_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "item_sources" in data
    assert len(data["items"]) == len(items_data)
    assert len(data["item_sources"]) == len(items_data)

    for i in range(len(items_data)):
        assert data["items"][i]["item"] == items_data[i]["item"]
        assert data["item_sources"][i]["external_id"] == items_data[i]["external_id"]
    
    return data

@pytest.mark.asyncio
async def test_create_items_bulk(client, create_test_embedding):
    plugin = await create_test_embedding()
    
    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    data = await bulk_create_helper(client, items_data, plugin)

@pytest.mark.asyncio
async def test_create_items_bulk_duplicates(client, create_test_embedding):
    plugin = await create_test_embedding()
    
    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": "bulk item 1", "external_id": "ext1"},
    ]

    data = await bulk_create_helper(client, items_data, plugin)

@pytest.mark.asyncio
async def test_create_items_bulk_conflict(client, create_test_item, create_test_embedding):
    item = await create_test_item()
    plugin = await create_test_embedding()
    
    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": item.item, "external_id": "ext1"},
    ]

    data = await bulk_create_helper(client, items_data, plugin)

@pytest.mark.asyncio
async def test_item_delete_propagation(client, db_session, create_test_item, create_test_embedding):
    item = await create_test_item()
    plugin = await create_test_embedding()
    source = await create_item_source(db_session, item, plugin, "ext123")
    
    # item/source record exists 
    response = await client.get(f"{api_str}/{item.id}")
    assert response.status_code == 200
    response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 200

    # delete item
    response = await client.delete(f"{api_str}/{item.id}")
    assert response.status_code == 200
    
    # check delete propagated to source 
    response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 404

    # check plugin still exists
    response = await client.get(f"{plugin_api_str}/{plugin.id}")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_plugin_delete_propagation(client, db_session, create_test_item, create_test_embedding):
    item = await create_test_item()
    plugin = await create_test_embedding()
    source = await create_item_source(db_session, item, plugin, "ext123")
    
    # item/source record exists 
    response = await client.get(f"{api_str}/{item.id}")
    assert response.status_code == 200
    response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 200

    # delete plugin
    response = await client.delete(f"{plugin_api_str}/{plugin.id}")
    assert response.status_code == 200

    # check plugin deleted 
    response = await client.get(f"{plugin_api_str}/{plugin.id}")
    assert response.status_code == 404
    
    # check delete propagated to source
    response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
    assert response.status_code == 404

    # check item still exists
    response = await client.get(f"{api_str}/{item.id}")
    assert response.status_code == 200

    # run cleanup
    response = await client.post(f"{api_str}/cleanup")
    assert response.status_code == 200
    data = response.json()
    assert "deleted_count" in data
    assert data["deleted_count"] >= 1  # Item should be deleted

    # confirm item deleted 
    response = await client.get(f"{api_str}/{item.id}")
    assert response.status_code == 404
