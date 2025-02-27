import pytest
import random 

@pytest.mark.asyncio
async def test_item_checkin(item_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

@pytest.mark.asyncio
async def test_item_checkin_duplicates(item_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": "bulk item 1", "external_id": "ext1"},
    ]

    results = await item_checkin(items_data, plugin.id)
    assert results['items'][0].item == results['items'][2].item
    assert results['items'][0].id == results['items'][2].id

@pytest.mark.asyncio
async def test_item_checkin_conflict(item_checkin, create_item_plugin_source):
    item, plugin, item_source = await create_item_plugin_source()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": item.item, "external_id": item_source.external_id},
    ]

    results = await item_checkin(items_data, plugin.id)





@pytest.mark.asyncio
async def test_score_checkin(item_checkin, score_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

    score_data = [{'item_id' : i.id, 'score' : random.random()}
                  for i in results['items']]
    score_records = await score_checkin(score_data, plugin.id)

@pytest.mark.asyncio
async def test_score_checkin_duplicates(item_checkin, score_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

    score_data = [{'item_id' : i.id, 'score' : random.random()}
                  for i in results['items']]
    score_data = list(score_data) + [score_data[0]]
    
    score_records = await score_checkin(score_data, plugin.id)

    assert score_records[0].item_id == score_records[2].item_id

@pytest.mark.asyncio
async def test_score_checkin_conflict(score_checkin, create_item,
                                      create_item_score, create_item_plugin_source):
    item1, plugin, item_source = await create_item_plugin_source()
    score = 9.42
    item_score = await create_item_score(item1.id, plugin.id, score=score)

    item2 = await create_item()
    score2 = 3.42

    score_data = [
        {'item_id' : item1.id, 'score' : score2}, # test score update
        {'item_id' : item2.id, 'score' : score2},
    ]

    score_records = await score_checkin(score_data, plugin.id)
    assert score_records[0].score == score2
