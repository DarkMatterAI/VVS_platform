import pytest
import itertools 
from vvs_database.crud import item_crud 

plugin_api_str = '/api/v1/plugins'

_item_counter = itertools.count(1)

@pytest.fixture(scope="function")
def create_item(db_session):    
    async def _create_item(name=None):
        name = name or f"Test Item {next(_item_counter)}"
        item = await item_crud.create_item(db_session, name)
        return item

    return _create_item

@pytest.fixture(scope="function")
def get_item(db_session):    
    async def _get_item(item_id):
        async with db_session.begin():
            result = await item_crud.get_item(db_session, item_id)
            return result 

    return _get_item

@pytest.fixture(scope="function")
def delete_item(db_session):    
    async def _delete_item(item):
        result = await item_crud.delete_item(db_session, item)
        return result 

    return _delete_item

@pytest.fixture(scope="function")
def create_item_source(db_session):    
    async def _create_item_source(item_id, plugin_id, external_id=None):
        item_source = await item_crud.create_item_source(db_session, item_id, plugin_id, external_id)
        return item_source

    return _create_item_source

@pytest.fixture(scope="function")
def create_item_plugin_source(create_item, create_test_embedding, create_item_source):    
    async def _create_item_plugin_source(item=None, external_id=None):
        item = await create_item(item)
        plugin = await create_test_embedding()
        item_source = await create_item_source(item.id, plugin.id, external_id)
        return item, plugin, item_source

    return _create_item_plugin_source

@pytest.fixture(scope="function")
def get_item_source(db_session):    
    async def _get_item_source(item_id, plugin_id):
        async with db_session.begin():
            result = await item_crud.get_item_source(db_session, item_id, plugin_id)
            return result

    return _get_item_source

@pytest.fixture(scope="function")
def delete_item_source(db_session):    
    async def _delete_item_source(item_source):
        result = await item_crud.delete_item_source(db_session, item_source)
        return result 

    return _delete_item_source

@pytest.fixture(scope="function")
def cleanup_items(db_session):    
    async def _cleanup_items():
        result = await item_crud.cleanup_unreferenced_items(db_session)
        return result 

    return _cleanup_items

@pytest.fixture(scope="function")
def create_item_score(db_session):    
    async def _create_item_score(item_id, plugin_id, score):
        item_score = await item_crud.create_item_score(db_session, item_id, plugin_id, score)
        return item_score

    return _create_item_score

@pytest.fixture(scope="function")
def get_item_score(db_session):    
    async def _get_item_score(item_id, plugin_id):
        async with db_session.begin():
            result = await item_crud.get_item_score(db_session, item_id, plugin_id)
            return result

    return _get_item_score

@pytest.fixture(scope="function")
def delete_item_score(db_session):    
    async def _delete_item_score(item_score):
        result = await item_crud.delete_item_score(db_session, item_score)
        return result 

    return _delete_item_score

@pytest.fixture(scope="function")
def item_checkin(db_session):    
    async def _item_checkin(new_items, plugin_id):
        new_items = [item_crud.NewItem(**i) for i in new_items]
        result = await item_crud.item_checkin(db_session, new_items, plugin_id)

        assert "items" in result
        assert "item_sources" in result 
        assert len(result["items"]) == len(new_items)
        assert len(result["item_sources"]) == len(new_items)

        for i in range(len(new_items)):
            assert result["items"][i].item == new_items[i].item
            assert result["item_sources"][i].external_id == new_items[i].external_id

        return result 


    return _item_checkin

@pytest.fixture(scope="function")
def score_checkin(db_session):    
    async def _score_checkin(new_scores, plugin_id):
        new_scores = [item_crud.NewScore(**i) for i in new_scores]
        result = await item_crud.score_checkin(db_session, new_scores, plugin_id)

        assert len(result) == len(new_scores)

        for i in range(len(new_scores)):
            assert result[i].item_id == new_scores[i].item_id
            assert result[i].score == new_scores[i].score

        return result 


    return _score_checkin