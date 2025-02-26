import pytest
import sqlalchemy
import random 

@pytest.mark.asyncio
async def test_item_score_create(create_item, create_test_embedding, create_item_score):
    item = await create_item()
    plugin = await create_test_embedding()
    score = 3.82
    item_score = await create_item_score(item.id, plugin.id, score=score)
    assert item_score.score == score
    assert item_score.item_id == item.id
    assert item_score.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_score_create_fails_invalid_ids(create_item_score):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        item_score = await create_item_score(10000000, 20000000, score=5.01)

@pytest.mark.asyncio
async def test_item_score_get(create_item_plugin_source, 
                              create_item_score, get_item_score):
    score = 10.1
    item, plugin, item_source = await create_item_plugin_source()
    item_score = await create_item_score(item.id, plugin.id, score=score)

    item_score_get = await get_item_score(item.id, plugin.id)
    assert item_score_get is not None
    assert item_score_get.score == item_score.score

@pytest.mark.asyncio
async def test_item_score_delete(create_item_plugin_source, create_item_score,
                                 get_item_score, delete_item_score):
    score = 10.1
    item, plugin, item_source = await create_item_plugin_source()
    item_score = await create_item_score(item.id, plugin.id, score=score)

    _ = await delete_item_score(item_score)

    item_score_get = await get_item_score(item.id, plugin.id)
    assert item_score_get is None 

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

@pytest.mark.asyncio
async def test_score_checkin_conflict(score_checkin, create_item,
                                      create_item_score, create_item_plugin_source):
    item1, plugin, item_source = await create_item_plugin_source()
    score = 9.42
    item_score = await create_item_score(item1.id, plugin.id, score=score)

    item2 = await create_item()
    score2 = 3.42

    score_data = [
        {'item_id' : item1.id, 'score' : score},
        {'item_id' : item2.id, 'score' : score2},
    ]

    score_records = await score_checkin(score_data, plugin.id)

@pytest.mark.asyncio
async def test_item_delete_score_propagation(create_item_plugin_source, create_item_score,
                                             delete_item, get_item_score, get_item_source,
                                             get_plugin):
    item, plugin, item_source = await create_item_plugin_source()
    score = 9.56
    item_score = await create_item_score(item.id, plugin.id, score=score)

    # delete item
    response = await delete_item(item)

    # check delete propagated to score 
    item_score = await get_item_score(item.id, plugin.id)
    assert item_score is None 

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check plugin still exists
    response = await get_plugin(plugin.id)
    assert response is not None 


@pytest.mark.asyncio
async def test_plugin_delete_score_propagation(create_item_plugin_source, create_item_score,
                                               get_item_source, get_item_score, get_item, cleanup_items,
                                               get_plugin, delete_plugin):
    item, plugin, item_source = await create_item_plugin_source()
    score = 9.56
    item_score = await create_item_score(item.id, plugin.id, score=score)

    # delete plugin
    response = await delete_plugin(plugin.id)

    # check plugin deleted 
    response = await get_plugin(plugin.id, with_error=False)
    assert response is None 

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check delete propagated to score 
    item_score = await get_item_score(item.id, plugin.id)
    assert item_score is None 

    # check item still exists
    item_record = await get_item(item.id)
    assert item_record is not None 

    # run cleanup
    deleted_count = await cleanup_items()
    assert deleted_count > 0

    # check item deleted
    item_record = await get_item(item.id)
    assert item_record is None 