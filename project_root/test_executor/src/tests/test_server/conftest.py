import pytest_asyncio
import itertools 

_item_counter = itertools.count(1)

@pytest_asyncio.fixture(scope="function")
def get_item_result(db_session):    
    async def _get_item_result(item_id, plugin_id):
        from vvs_database import crud
        async with db_session.begin():
            result = await crud.get_item_result(db_session, item_id, plugin_id)
            return result

    return _get_item_result

@pytest_asyncio.fixture(scope="function")
async def get_item_source(db_session):    
    async def _get_item_source(item_id, plugin_id):
        from vvs_database import crud
        async with db_session.begin():
            result = await crud.get_item_source(db_session, item_id, plugin_id)
            return result

    return _get_item_source

@pytest_asyncio.fixture(scope="function")
async def get_item_by_name(db_session):    
    async def _get_item_by_name(item):
        from vvs_database import crud
        async with db_session.begin():
            result = await crud.get_item_by_name(db_session, item)
            return result

    return _get_item_by_name

@pytest_asyncio.fixture(scope="function")
async def get_assembly_by_product_plugin(db_session):
    async def _get_assembly_by_product_plugin(product_id, plugin_id):
        from vvs_database import crud
        async with db_session.begin():
            result = await crud.get_assembly_by_product_plugin(db_session, product_id, plugin_id)
        return result

    return _get_assembly_by_product_plugin
