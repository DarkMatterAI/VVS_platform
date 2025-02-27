import pytest
import sqlalchemy

@pytest.mark.asyncio
async def test_item_result_create(create_item, create_test_embedding, create_item_result):
    item = await create_item()
    plugin = await create_test_embedding()
    score = 3.82
    embedding = [0.1, 0.2, 0.3]
    item_result = await create_item_result(
        item.id, plugin.id, valid=True, score=score, embedding=embedding
    )
    assert item_result.valid is True
    assert item_result.score == score
    assert item_result.embedding == embedding
    assert item_result.item_id == item.id
    assert item_result.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_result_create_no_score(create_item, create_test_embedding, create_item_result):
    item = await create_item()
    plugin = await create_test_embedding()
    embedding = [0.1, 0.2, 0.3]
    item_result = await create_item_result(
        item.id, plugin.id, valid=False, embedding=embedding
    )
    assert item_result.valid is False
    assert item_result.score is None
    assert item_result.embedding == embedding
    assert item_result.item_id == item.id
    assert item_result.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_result_create_fails_invalid_ids(create_item_result):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        item_result = await create_item_result(10000000, 20000000, valid=True, score=5.01)

@pytest.mark.asyncio
async def test_item_result_get(create_item_plugin_source, 
                              create_item_result, get_item_result):
    score = 10.1
    item, plugin, item_source = await create_item_plugin_source()
    item_result = await create_item_result(item.id, plugin.id, valid=True, score=score)

    item_result_get = await get_item_result(item.id, plugin.id)
    assert item_result_get is not None
    assert item_result_get.valid is True
    assert item_result_get.score == item_result.score

@pytest.mark.asyncio
async def test_item_result_delete(create_item_plugin_source, create_item_result,
                                get_item_result, delete_item_result):
    score = 10.1
    item, plugin, item_source = await create_item_plugin_source()
    item_result = await create_item_result(item.id, plugin.id, valid=True, score=score)

    _ = await delete_item_result(item_result)

    item_result_get = await get_item_result(item.id, plugin.id)
    assert item_result_get is None 

@pytest.mark.asyncio
async def test_item_delete_result_propagation(create_item_plugin_source, create_item_result,
                                             delete_item, get_item_result, get_item_source,
                                             get_plugin):
    item, plugin, item_source = await create_item_plugin_source()
    score = 9.56
    item_result = await create_item_result(item.id, plugin.id, valid=True, score=score)

    # delete item
    response = await delete_item(item)

    # check delete propagated to result 
    item_result = await get_item_result(item.id, plugin.id)
    assert item_result is None 

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check plugin still exists
    response = await get_plugin(plugin.id)
    assert response is not None 


@pytest.mark.asyncio
async def test_plugin_delete_result_propagation(create_item_plugin_source, create_item_result,
                                              get_item_source, get_item_result, get_item, cleanup_items,
                                              get_plugin, delete_plugin):
    item, plugin, item_source = await create_item_plugin_source()
    score = 9.56
    item_result = await create_item_result(item.id, plugin.id, valid=True, score=score)

    # delete plugin
    response = await delete_plugin(plugin.id)

    # check plugin deleted 
    response = await get_plugin(plugin.id, with_error=False)
    assert response is None 

    # check propagated to source
    item_source = await get_item_source(item.id, plugin.id)
    assert item_source is None 

    # check delete propagated to result 
    item_result = await get_item_result(item.id, plugin.id)
    assert item_result is None 

    # check item still exists
    item_record = await get_item(item.id)
    assert item_record is not None 

    # run cleanup
    deleted_count = await cleanup_items()
    assert deleted_count > 0

    # check item deleted
    item_record = await get_item(item.id)
    assert item_record is None
