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
_assembly_counter = itertools.count(1)

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
    async def _create_embedding(name=None, plugin_class='generic',
                                vector_length=128, distance_metric="Cosine"):
        embedding = schemas.EmbeddingPluginCreate(
            name=name or f"Test Embedding {next(_embedding_counter)}",
            plugin_class=plugin_class,
            type="embedding",
            execution_type="queue",
            group_key="test",
            timeout=30,
            max_concurrency=5,
            max_retries=1,
            batch_size=1,
            vector_length=vector_length,
            distance_metric=distance_metric
        )
        embedding = await crud.create_plugin(db_session, embedding)
        return embedding

    return _create_embedding

@pytest_asyncio.fixture(scope="function")
async def create_test_assembly_plugin(db_session):    
    async def _create_assembly_plugin(name=None, num_parents=2):
        assembly_plugin = schemas.AssemblyPluginCreate(
            name=name or f"Test Assembly Plugin {next(_assembly_counter)}",
            plugin_class="generic",
            type="assembly",
            execution_type="queue",
            group_key="test",
            timeout=30,
            max_concurrency=5,
            max_retries=1,
            batch_size=1,
            num_parents=num_parents
        )
        plugin = await crud.create_plugin(db_session, assembly_plugin)
        return plugin

    return _create_assembly_plugin

@pytest_asyncio.fixture(scope="function")
async def get_plugin(db_session):    
    async def _get_plugin(plugin_id, with_error=True):
        async with db_session.begin():
            result = await crud.get_plugin(db_session, plugin_id, with_error=with_error)
            return result 

    return _get_plugin

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
async def get_items(db_session):    
    async def _get_items(item_ids):
        async with db_session.begin():
            result = await crud.get_items(db_session, item_ids)
            return result 

    return _get_items

@pytest_asyncio.fixture(scope="function")
async def create_item_plugin_source(db_session, create_item, create_test_embedding):    
    async def _create_item_plugin_source(item=None, external_id=None):
        item = await create_item(item)
        plugin = await create_test_embedding()
        item_source = await crud.create_item_source(db_session, item.id, plugin.id, external_id)
        # item_source = await create_item_source(item.id, plugin.id, external_id)
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
async def get_item_sources(db_session):    
    async def _get_item_sources(item_ids, plugin_id):
        async with db_session.begin():
            result = await crud.get_item_sources(db_session, item_ids, plugin_id)
            return result

    return _get_item_sources

@pytest_asyncio.fixture(scope="function")
def get_item_result(db_session):    
    async def _get_item_result(item_id, plugin_id):
        async with db_session.begin():
            result = await crud.get_item_result(db_session, item_id, plugin_id)
            return result

    return _get_item_result

@pytest_asyncio.fixture(scope="function")
def get_item_results(db_session):    
    async def _get_item_results(item_ids, plugin_id):
        async with db_session.begin():
            result = await crud.get_item_results(db_session, item_ids, plugin_id)
            return result

    return _get_item_results

@pytest_asyncio.fixture(scope="function")
async def get_assembly_by_id(db_session):
    async def _get_assembly_by_id(assembly_id):
        async with db_session.begin():
            result = await crud.get_assembly_by_id(db_session, assembly_id)
        return result

    return _get_assembly_by_id

@pytest_asyncio.fixture(scope="function")
async def get_assembly_by_product_plugin(db_session):
    async def _get_assembly_by_product_plugin(product_id, plugin_id):
        async with db_session.begin():
            result = await crud.get_assembly_by_product_plugin(db_session, product_id, plugin_id)
        return result

    return _get_assembly_by_product_plugin

@pytest_asyncio.fixture(scope="function")
async def get_assemblies_by_component(db_session):
    async def _get_assemblies_by_component(component_id):
        async with db_session.begin():
            result = await crud.get_assemblies_by_component(db_session, component_id)
        return result

    return _get_assemblies_by_component

@pytest_asyncio.fixture(scope="function")
async def get_assemblies_by_component_key(db_session):
    async def _get_assemblies_by_component_key(component_key):
        async with db_session.begin():
            result = await crud.get_assemblies_by_component_key(db_session, component_key)
        return result

    return _get_assemblies_by_component_key

@pytest_asyncio.fixture(scope="function")
async def get_assemblies_by_component_keys(db_session):
    async def _get_assemblies_by_component_keys(component_keys):
        async with db_session.begin():
            result = await crud.get_assemblies_by_component_keys(db_session, component_keys)
        return result

    return _get_assemblies_by_component_keys

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

@pytest_asyncio.fixture(scope="function")
async def assembly_checkin(db_session):    
    async def _assembly_checkin(new_assemblies, plugin_id):
        new_assemblies = [schemas.NewAssembly(**a) for a in new_assemblies]
        result = await crud.assembly_checkin(db_session, new_assemblies, plugin_id)

        # Verify essentials of the returned data
        assert "items" in result
        assert "item_sources" in result
        assert "assemblies" in result
        assert len(result["items"]) == len(new_assemblies)
        assert len(result["assemblies"]) == len(new_assemblies)

        # Check that the items match the input
        for i, assembly in enumerate(new_assemblies):
            assert result["items"][i].item == assembly.item

        return result 

    return _assembly_checkin
