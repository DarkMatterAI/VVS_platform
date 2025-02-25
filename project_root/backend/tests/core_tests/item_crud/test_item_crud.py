import pytest

plugin_api_str = '/api/v1/plugins'


@pytest.mark.asyncio
async def test_item_create(client, create_item):
    item = 'item_create_test_1'
    item_record = await create_item(item)
    assert item_record.item == item

@pytest.mark.asyncio
async def test_item_get(client, create_item, get_item):
    item_record = await get_item(999999)
    assert item_record is None 

    item = await create_item()
    item_record = await get_item(item.id)
    assert item_record is not None

@pytest.mark.asyncio
async def test_item_delete(client, create_item, get_item, delete_item):
    item = await create_item()

    await delete_item(item)
    item_record = await get_item(item.id)
    assert item_record is None 







































#### original do not touch


# import pytest
# import random 
# from app import models

# api_str = '/api/v1/item'
# plugin_api_str = '/api/v1/plugins'

# @pytest.mark.asyncio
# async def test_get_item_success(client, create_test_item):
#     item_name = 'item_create_test_1'
#     item = await create_test_item(item_name)
    
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200
#     data = response.json()
#     assert data["item"] == item_name
#     assert data["id"] == item.id
#     assert "created_at" in data

# @pytest.mark.asyncio
# async def test_get_item_not_found(client):
#     response = await client.get(f"{api_str}/99999999")
#     assert response.status_code == 404
#     assert response.json()["detail"] == "Item not found"

# @pytest.mark.asyncio
# async def test_delete_item_success(client, create_test_item):
#     item_name = 'item_delete_test_1'
#     item = await create_test_item(item_name)
    
#     response = await client.delete(f"{api_str}/{item.id}")
#     assert response.status_code == 200
#     data = response.json()
#     assert data["item"] == item_name
    
#     # Verify item is deleted
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 404

# @pytest.mark.asyncio
# async def test_delete_item_not_found(client):
#     response = await client.delete(f"{api_str}/99999999")
#     assert response.status_code == 404
#     assert response.json()["detail"] == "Item not found"

# @pytest.mark.asyncio
# async def test_get_item_source_success(client, db_session, create_test_item, 
#                                        create_test_item_source, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     source = await create_test_item_source(item, plugin, external_id="ext123")
    
#     response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 200
#     data = response.json()
#     assert data["item_id"] == item.id
#     assert data["plugin_id"] == plugin.id
#     assert data["external_id"] == "ext123"

# @pytest.mark.asyncio
# async def test_get_item_source_not_found(client):
#     response = await client.get(f"{api_str}/999/sources/999")
#     assert response.status_code == 404
#     assert response.json()["detail"] == "Item source not found"

# @pytest.mark.asyncio
# async def test_delete_item_source_success(client, db_session, create_test_item, 
#                                           create_test_item_source, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     source = await create_test_item_source(item, plugin, external_id="ext123")
    
#     response = await client.delete(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 200
    
#     response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 404

# @pytest.mark.asyncio
# async def test_cleanup_items(client, db_session, create_test_item, 
#                              create_test_item_source, create_test_embedding):
#     item1 = await create_test_item()
#     item2 = await create_test_item()
#     plugin = await create_test_embedding()
#     source = await create_test_item_source(item1, plugin, external_id="ext123")
    
#     response = await client.post(f"{api_str}/cleanup")
#     assert response.status_code == 200
#     data = response.json()
#     assert "deleted_count" in data
#     assert data["deleted_count"] >= 1  # Should delete at least item2

#     response = await client.get(f"{api_str}/{item1.id}") # item1 should still exist
#     assert response.status_code == 200

#     response = await client.get(f"{api_str}/{item1.id}/sources/{plugin.id}") # source should still exist
#     assert response.status_code == 200

#     response = await client.get(f"{api_str}/{item2.id}") # item2 should be deleted 
#     assert response.status_code == 404

# @pytest.mark.asyncio
# async def test_create_items_bulk_invalid_plugin(client):
#     items_data = [
#         {"item": "bulk item 1", "external_id": "ext1"}
#     ]
    
#     response = await client.post(
#         f"{api_str}/item_checkin?plugin_id=99999999",
#         json=items_data
#     )
    
#     assert response.status_code == 404

# async def bulk_item_create_helper(client, items_data, plugin):
#     response = await client.post(
#         f"{api_str}/item_checkin?plugin_id={plugin.id}",
#         json=items_data
#     )
    
#     assert response.status_code == 200
#     data = response.json()
#     assert "items" in data
#     assert "item_sources" in data
#     assert len(data["items"]) == len(items_data)
#     assert len(data["item_sources"]) == len(items_data)

#     for i in range(len(items_data)):
#         assert data["items"][i]["item"] == items_data[i]["item"]
#         assert data["item_sources"][i]["external_id"] == items_data[i]["external_id"]
    
#     return data

# @pytest.mark.asyncio
# async def test_create_items_bulk(client, create_test_embedding):
#     plugin = await create_test_embedding()
    
#     items_data = [
#         {"item": "bulk item 1", "external_id": "ext1"},
#         {"item": "bulk item 2", "external_id": "ext2"}
#     ]

#     data = await bulk_item_create_helper(client, items_data, plugin)

# @pytest.mark.asyncio
# async def test_create_items_bulk_duplicates(client, create_test_embedding):
#     plugin = await create_test_embedding()
    
#     items_data = [
#         {"item": "bulk item 1", "external_id": "ext1"},
#         {"item": "bulk item 2", "external_id": "ext2"},
#         {"item": "bulk item 1", "external_id": "ext1"},
#     ]

#     data = await bulk_item_create_helper(client, items_data, plugin)

# @pytest.mark.asyncio
# async def test_create_items_bulk_conflict(client, create_test_item, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
    
#     items_data = [
#         {"item": "bulk item 1", "external_id": "ext1"},
#         {"item": "bulk item 2", "external_id": "ext2"},
#         {"item": item.item, "external_id": "ext1"},
#     ]

#     data = await bulk_item_create_helper(client, items_data, plugin)

# @pytest.mark.asyncio
# async def test_item_delete_source_propagation(client, create_test_item, 
#                                        create_test_item_source, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     source = await create_test_item_source(item, plugin, external_id="ext123")
    
#     # item/source record exists 
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200
#     response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 200

#     # delete item
#     response = await client.delete(f"{api_str}/{item.id}")
#     assert response.status_code == 200
    
#     # check delete propagated to source 
#     response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 404

#     # check plugin still exists
#     response = await client.get(f"{plugin_api_str}/{plugin.id}")
#     assert response.status_code == 200


# @pytest.mark.asyncio
# async def test_plugin_delete_source_propagation(client, create_test_item, 
#                                          create_test_item_source, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     source = await create_test_item_source(item, plugin, external_id="ext123")
    
#     # item/source record exists 
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200
#     response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 200

#     # delete plugin
#     response = await client.delete(f"{plugin_api_str}/{plugin.id}")
#     assert response.status_code == 200

#     # check plugin deleted 
#     response = await client.get(f"{plugin_api_str}/{plugin.id}")
#     assert response.status_code == 404
    
#     # check delete propagated to source
#     response = await client.get(f"{api_str}/{item.id}/sources/{plugin.id}")
#     assert response.status_code == 404

#     # check item still exists
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200

#     # run cleanup
#     response = await client.post(f"{api_str}/cleanup")
#     assert response.status_code == 200
#     data = response.json()
#     assert "deleted_count" in data
#     assert data["deleted_count"] >= 1  # Item should be deleted

#     # confirm item deleted 
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 404

# @pytest.mark.asyncio
# async def test_get_score_success(client, create_test_item, create_test_score, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     score_value = 8.32
#     score = await create_test_score(item, plugin, score_value)
    
#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 200
#     data = response.json()
#     assert data["score"] == score_value
#     assert data["item_id"] == item.id
#     assert data["plugin_id"] == plugin.id
#     assert "created_at" in data

# @pytest.mark.asyncio
# async def test_delete_score_success(client, create_test_item, create_test_score, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     score_value = 8.32
#     score = await create_test_score(item, plugin, score_value)
    
#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 200

#     response = await client.delete(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 200

#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 404

# async def bulk_score_create_helper(client, score_data, plugin):
#     response = await client.post(
#         f"{api_str}/score_checkin?plugin_id={plugin.id}",
#         json=score_data
#     )
    
#     assert response.status_code == 200
#     data = response.json()
#     assert len(data) == len(score_data)

#     for i in range(len(score_data)):
#         assert data[i]['item_id'] == score_data[i]['item_id']
#         assert data[i]['score'] == score_data[i]['score']
#         assert data[i]['plugin_id'] == plugin.id

#     return data

# @pytest.mark.asyncio
# async def test_create_scores_bulk(client, create_test_embedding):
#     plugin = await create_test_embedding()
    
#     items_data = [
#         {"item": "bulk item 1", "external_id": "ext1"},
#         {"item": "bulk item 2", "external_id": "ext2"}
#     ]

#     item_records = await bulk_item_create_helper(client, items_data, plugin)

#     score_data = [{'item_id' : i['id'], 'score' : random.random()}
#                    for i in item_records['items']]
    
#     score_records = await bulk_score_create_helper(client, score_data, plugin)

# @pytest.mark.asyncio
# async def test_create_scores_missing_item_fails(client, create_test_embedding):
#     plugin = await create_test_embedding()
#     score_data = [{'item_id' : 999999, 'score' : random.random()}]
    
#     response = await client.post(
#         f"{api_str}/score_checkin?plugin_id={plugin.id}",
#         json=score_data
#     )
    
#     assert response.status_code == 404

# @pytest.mark.asyncio
# async def test_create_scores_missing_plugin_fails(client, create_test_item):
#     item = await create_test_item()
#     score_data = [{'item_id' : item.id, 'score' : random.random()}]
    
#     response = await client.post(
#         f"{api_str}/score_checkin?plugin_id=9999999",
#         json=score_data
#     )
    
#     assert response.status_code == 404

# @pytest.mark.asyncio
# async def test_create_scores_bulk_duplicates(client, create_test_embedding):
#     plugin = await create_test_embedding()
    
#     items_data = [
#         {"item": "bulk item 1", "external_id": "ext1"},
#         {"item": "bulk item 2", "external_id": "ext2"}
#     ]

#     item_records = await bulk_item_create_helper(client, items_data, plugin)

#     score_data = [{'item_id' : i['id'], 'score' : random.random()}
#                    for i in item_records['items']]
#     score_data = list(score_data) + [score_data[0]]
    
#     score_records = await bulk_score_create_helper(client, score_data, plugin)

# @pytest.mark.asyncio
# async def test_create_scores_bulk_conflict(client, create_test_item, 
#                                            create_test_score, create_test_embedding):
#     item1 = await create_test_item()
#     plugin = await create_test_embedding()
#     score_value1 = 8.32
#     score1 = await create_test_score(item1, plugin, score_value1)

#     item2 = await create_test_item()
#     score_value2 = 1.23

#     score_data = [
#         {'item_id' : item1.id, 'score' : score_value1},
#         {'item_id' : item2.id, 'score' : score_value2},
#     ]

#     score_records = await bulk_score_create_helper(client, score_data, plugin)
    
# @pytest.mark.asyncio
# async def test_item_delete_score_propagation(client, create_test_item, create_test_score, 
#                                              create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     score_value = 8.32
#     score = await create_test_score(item, plugin, score_value)

#     # item/score record exists 
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200
#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 200

#     # delete item
#     response = await client.delete(f"{api_str}/{item.id}")
#     assert response.status_code == 200
    
#     # check delete propagated to score 
#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 404

#     # check plugin still exists
#     response = await client.get(f"{plugin_api_str}/{plugin.id}")
#     assert response.status_code == 200


# @pytest.mark.asyncio
# async def test_plugin_delete_score_propagation(client, create_test_item, 
#                                                create_test_score, create_test_embedding):
#     item = await create_test_item()
#     plugin = await create_test_embedding()
#     score_value = 8.32
#     score = await create_test_score(item, plugin, score_value)
    
#     # item/score record exists 
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200
#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 200

#     # delete plugin
#     response = await client.delete(f"{plugin_api_str}/{plugin.id}")
#     assert response.status_code == 200

#     # check plugin deleted 
#     response = await client.get(f"{plugin_api_str}/{plugin.id}")
#     assert response.status_code == 404
    
#     # check delete propagated to source
#     response = await client.get(f"{api_str}/{item.id}/scores/{plugin.id}")
#     assert response.status_code == 404

#     # check item still exists
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 200

#     # run cleanup
#     response = await client.post(f"{api_str}/cleanup")
#     assert response.status_code == 200
#     data = response.json()
#     assert "deleted_count" in data
#     assert data["deleted_count"] >= 1  # Item should be deleted

#     # confirm item deleted 
#     response = await client.get(f"{api_str}/{item.id}")
#     assert response.status_code == 404





# # delete propagation interaction -  wait for assembly 
