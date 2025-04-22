import pytest
import sqlalchemy

from vvs_database import crud 

@pytest.mark.asyncio
async def test_item_result_create(db_session,
                                  create_item, 
                                  create_test_embedding):
    item = await create_item()
    plugin = await create_test_embedding()
    score = 3.82
    embedding = [0.1, 0.2, 0.3]
    item_result = await crud.create_item_result(db_session, item.id, plugin.id, 
                                                valid=True, score=score, embedding=embedding)
    assert item_result.valid is True
    assert item_result.score == score
    assert item_result.embedding == embedding
    assert item_result.item_id == item.id
    assert item_result.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_result_create_no_score(db_session,
                                           create_item, 
                                           create_test_embedding):
    item = await create_item()
    plugin = await create_test_embedding()
    embedding = [0.1, 0.2, 0.3]
    item_result = await crud.create_item_result(db_session, item.id, plugin.id, 
                                                valid=False, score=None, embedding=embedding)
    assert item_result.valid is False
    assert item_result.score is None
    assert item_result.embedding == embedding
    assert item_result.item_id == item.id
    assert item_result.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_item_result_create_fails_invalid_ids(db_session):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        item_result = await crud.create_item_result(db_session, 10000000, 20000000, 
                                                    valid=True, score=5.01)

@pytest.mark.asyncio
async def test_item_result_get(db_session,
                               create_item_plugin_source):
    score = 10.1
    item, plugin, item_source = await create_item_plugin_source()
    item_result = await crud.create_item_result(db_session, item.id, plugin.id, valid=True, score=score)

    item_result_get = await crud.get_item_result(db_session, item.id, plugin.id)
    assert item_result_get is not None
    assert item_result_get.valid is True
    assert item_result_get.score == item_result.score
    await db_session.commit()

@pytest.mark.asyncio
async def test_get_item_results(db_session,
                                create_item, 
                                create_test_embedding):
    
    plugin = await create_test_embedding()
    items = []
    item_results = []
    for i in range(3):
        item = await create_item()
        item_source = await crud.create_item_source(db_session, item.id, plugin.id, f"test_get_item_results_{i}")
        score = 10 + 0.5*i 
        item_result = await crud.create_item_result(db_session, item.id, plugin.id, valid=True, score=score)

        items.append(item)
        item_results.append(item_result)

    item_results_get = await crud.get_item_results(db_session, [i.id for i in items], plugin.id)
    assert len(item_results_get) == len(items)
    await db_session.commit()


@pytest.mark.asyncio
async def test_item_result_delete(db_session,
                                  create_item_plugin_source):
    score = 10.1
    item, plugin, item_source = await create_item_plugin_source()
    item_result = await crud.create_item_result(db_session, item.id, plugin.id, valid=True, score=score)

    _ = await crud.delete_item_result(db_session, item_result)

    item_result_get = await crud.get_item_result(db_session, item.id, plugin.id)
    assert item_result_get is None 
    await db_session.commit()

@pytest.mark.asyncio
async def test_item_delete_result_propagation(db_session, 
                                              create_item_plugin_source):
    item, plugin, item_source = await create_item_plugin_source()
    score = 9.56
    item_result = await crud.create_item_result(db_session, item.id, plugin.id, valid=True, score=score)

    # delete item
    _ = await crud.delete_item(db_session, item)

    # check delete propagated to result 
    item_result = await crud.get_item_result(db_session, item.id, plugin.id)
    assert item_result is None 

    # check propagated to source
    item_source = await crud.get_item_source(db_session, item.id, plugin.id)
    assert item_source is None 

    # check plugin still exists
    response = await crud.get_plugin(db_session, plugin.id)
    assert response is not None 


@pytest.mark.asyncio
async def test_plugin_delete_result_propagation(db_session, 
                                                create_item_plugin_source):
    item, plugin, item_source = await create_item_plugin_source()
    score = 9.56
    item_result = await crud.create_item_result(db_session, item.id, plugin.id, valid=True, score=score)

    # delete plugin
    result = await crud.delete_plugin(db_session, plugin.id)

    # check plugin deleted 
    response = await crud.get_plugin(db_session, plugin.id, with_error=False)
    assert response is None 

    # check propagated to source
    item_source = await crud.get_item_source(db_session, item.id, plugin.id)
    assert item_source is None 

    # check delete propagated to result 
    item_result = await crud.get_item_result(db_session, item.id, plugin.id)
    assert item_result is None 

    # check item still exists
    item_record = await crud.get_item(db_session, item.id)
    assert item_record is not None 

    # run cleanup
    deleted_count = await crud.cleanup_unreferenced_items(db_session)
    assert deleted_count > 0

    # check item deleted
    item_record = await crud.get_item(db_session, item.id)
    assert item_record is None
    await db_session.commit()
