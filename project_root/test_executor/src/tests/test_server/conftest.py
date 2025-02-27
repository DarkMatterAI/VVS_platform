import pytest
import pytest_asyncio
import asyncio
import itertools 
import string 
import numpy as np 
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from vvs_database import settings

_item_counter = itertools.count(1)

@pytest_asyncio.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    engine = create_async_engine(settings.SQLALCHEMY_DATABASE_URL)
    yield engine
    await engine.dispose()

@pytest_asyncio.fixture(scope="function")
async def db_session(test_engine):
    """Create an async database session for testing."""
    Session = sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with Session() as session:
        try:
            yield session
        finally:
            await session.rollback()
            await asyncio.sleep(0)
            await session.close()

@pytest_asyncio.fixture(scope="function")
async def create_test_item(db_session):
    async def _create_item(name=None, add_key=True):
        from vvs_database import crud

        name = name or f"Test Item {next(_item_counter)}"

        if add_key:
            item_key = ''.join(np.random.choice([i for i in string.ascii_lowercase], 16))
            name = f"{name} {item_key}"

        item = await crud.create_item(db_session, name)
        return item
    return _create_item

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
