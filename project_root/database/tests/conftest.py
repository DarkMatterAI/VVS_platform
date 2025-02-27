import pytest
import pytest_asyncio
import asyncio
import itertools 

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from vvs_database import Base, settings, schemas, crud
from vvs_database.testing import create_test_database_url, drop_test_database

_item_counter = itertools.count(1)
_embedding_counter = itertools.count(1)

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    # Create test database
    test_db_url = await create_test_database_url(
        settings.DEFAULT_DB_URL,
        settings.POSTGRES_DB_TEST
    )
    
    # Create test engine
    test_engine = create_async_engine(test_db_url)
    
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield test_engine

    # Cleanup
    await test_engine.dispose()
    
    # Drop test database
    await drop_test_database(
        settings.DEFAULT_DB_URL,
        settings.POSTGRES_DB_TEST
    )


@pytest_asyncio.fixture(scope="function")
async def db_session(test_engine):
    TestingSessionLocal = sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with TestingSessionLocal() as session:
        try:
            yield session
        finally:
            await session.rollback()
            await session.close()

@pytest_asyncio.fixture(scope="function")
async def create_test_embedding(db_session):
    counter = itertools.count(1)
    
    async def _create_embedding(name=None, plugin_class='generic',
                                vector_length=128, distance_metric="Cosine"):
        embedding = schemas.EmbeddingPluginCreate(
            name=name or f"Test Embedding {next(counter)}",
            plugin_class=plugin_class,
            type="embedding",
            execution_type="queue",
            group_key="test",
            timeout=30,
            max_concurrency=5,
            max_retries=1,
            vector_length=vector_length,
            distance_metric=distance_metric
        )
        embedding = await crud.create_plugin(db_session, embedding)
        return embedding

    return _create_embedding

@pytest_asyncio.fixture(scope="function")
async def get_plugin(db_session):    
    async def _get_plugin(plugin_id, with_error=True):
        async with db_session.begin():
            result = await crud.get_plugin(db_session, plugin_id, with_error=with_error)
            return result 

    return _get_plugin

@pytest_asyncio.fixture(scope="function")
async def create_plugin(db_session):    
    async def _create_plugin(plugin_data, response_model=False):
        result = await crud.create_plugin(db_session, plugin_data, response_model=response_model)
        return result 

    return _create_plugin

@pytest_asyncio.fixture(scope="function")
async def delete_plugin(db_session):    
    async def _delete_plugin(plugin_id):
        result = await crud.delete_plugin(db_session, plugin_id)
        return result 

    return _delete_plugin

@pytest_asyncio.fixture(scope="function")
async def create_item(db_session):
    async def _create_item(name=None):
        name = name or f"Test Item {next(_item_counter)}"
        item = await crud.create_item(db_session, name)
        return item
    return _create_item

@pytest_asyncio.fixture(scope="function")
async def get_item(db_session):    
    async def _get_item(item_id):
        async with db_session.begin():
            result = await crud.get_item(db_session, item_id)
            return result 

    return _get_item

@pytest_asyncio.fixture(scope="function")
async def delete_item(db_session):    
    async def _delete_item(item):
        result = await crud.delete_item(db_session, item)
        return result 

    return _delete_item

@pytest_asyncio.fixture(scope="function")
async def create_item_source(db_session):    
    async def _create_item_source(item_id, plugin_id, external_id=None):
        item_source = await crud.create_item_source(db_session, item_id, plugin_id, external_id)
        return item_source

    return _create_item_source

@pytest_asyncio.fixture(scope="function")
async def create_item_plugin_source(create_item, create_test_embedding, create_item_source):    
    async def _create_item_plugin_source(item=None, external_id=None):
        item = await create_item(item)
        plugin = await create_test_embedding()
        item_source = await create_item_source(item.id, plugin.id, external_id)
        return item, plugin, item_source

    return _create_item_plugin_source

@pytest_asyncio.fixture(scope="function")
async def get_item_source(db_session):    
    async def _get_item_source(item_id, plugin_id):
        async with db_session.begin():
            result = await crud.get_item_source(db_session, item_id, plugin_id)
            return result

    return _get_item_source

@pytest_asyncio.fixture(scope="function")
async def delete_item_source(db_session):    
    async def _delete_item_source(item_source):
        result = await crud.delete_item_source(db_session, item_source)
        return result 

    return _delete_item_source

@pytest_asyncio.fixture(scope="function")
async def cleanup_items(db_session):    
    async def _cleanup_items():
        result = await crud.cleanup_unreferenced_items(db_session)
        return result 

    return _cleanup_items

@pytest_asyncio.fixture(scope="function")
async def create_item_result(db_session):    
    async def _create_item_result(item_id, plugin_id, valid, score=None, embedding=None):
        item_result = await crud.create_item_result(
            db_session, item_id, plugin_id, valid, score, embedding
        )
        return item_result

    return _create_item_result

@pytest_asyncio.fixture(scope="function")
def get_item_result(db_session):    
    async def _get_item_result(item_id, plugin_id):
        async with db_session.begin():
            result = await crud.get_item_result(db_session, item_id, plugin_id)
            return result

    return _get_item_result

@pytest_asyncio.fixture(scope="function")
async def delete_item_result(db_session):    
    async def _delete_item_result(item_result):
        result = await crud.delete_item_result(db_session, item_result)
        return result 

    return _delete_item_result

@pytest_asyncio.fixture(scope="function")
async def item_checkin(db_session):    
    async def _item_checkin(new_items, plugin_id):
        new_items = [schemas.NewItem(**i) for i in new_items]
        result = await crud.item_checkin(db_session, new_items, plugin_id)

        assert "items" in result
        assert "item_sources" in result 
        assert len(result["items"]) == len(new_items)
        assert len(result["item_sources"]) == len(new_items)

        for i in range(len(new_items)):
            assert result["items"][i].item == new_items[i].item
            assert result["item_sources"][i].external_id == new_items[i].external_id

        return result 


    return _item_checkin

@pytest_asyncio.fixture(scope="function")
async def result_checkin(db_session):    
    async def _result_checkin(new_results, plugin_id):
        new_results = [schemas.NewResult(**i) for i in new_results]
        result = await crud.result_checkin(db_session, new_results, plugin_id)

        assert len(result) == len(new_results)

        for i in range(len(new_results)):
            assert result[i].item_id == new_results[i].item_id
            assert result[i].valid == new_results[i].valid
            assert result[i].score == new_results[i].score
            # For embeddings, we either both have them or neither has them
            if new_results[i].embedding is None:
                assert result[i].embedding is None
            else:
                assert result[i].embedding == new_results[i].embedding

        return result 

    return _result_checkin
