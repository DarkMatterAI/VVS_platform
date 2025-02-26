import asyncio
import pytest
import itertools 
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.core.database import get_db

from vvs_database import settings 
from vvs_database import Base, schemas
from vvs_database.testing import create_test_database_url, drop_test_database

_embedding_counter = itertools.count(1)

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
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

@pytest.fixture(scope="function")
async def db_session(test_engine):
    # Keep your original session management
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

@pytest.fixture(scope="function")
async def client(db_session):
    # Keep your original client setup
    async def override_get_db():
        try:
            yield db_session
        finally:
            await db_session.close()

    app.dependency_overrides[get_db] = override_get_db
    
    async with AsyncClient(
        transport=ASGITransport(app=app), 
        base_url="http://test"
    ) as ac:
        yield ac
    
    app.dependency_overrides.clear()


@pytest.fixture(scope="function")
def create_test_embedding(client):
    async def _create_embedding(name=None, vector_length=128, distance_metric="Cosine"):
        response = await client.post(
            f"/api/v1/plugins/",
            json={
                "name": name or f"Test Embedding {next(_embedding_counter)}",
                "plugin_class": "generic",
                "type": "embedding",
                "execution_type": "queue",
                "group_key": "test",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "vector_length": vector_length,
                "distance_metric": distance_metric
            }
        )
        assert response.status_code == 200
        return schemas.EmbeddingPluginInDB(**response.json())

    return _create_embedding



# # _item_counter = itertools.count(1)

# # @pytest.fixture(scope="function")
# # def create_test_item(db_session):    
# #     async def _create_item(name=None):
# #         item = models.Item(item=name or f"Test Item {next(_item_counter)}")
# #         db_session.add(item)
# #         await db_session.commit()
# #         return item

# #     return _create_item

# # @pytest.fixture(scope="function")
# # def create_test_item_source(db_session):    
# #     async def _create_item_source(item, plugin, external_id=None):
# #         source = models.ItemSource(
# #             item_id=item.id,
# #             plugin_id=plugin.id,
# #             external_id=external_id
# #         )
# #         db_session.add(source)
# #         await db_session.commit()
# #         return source

# #     return _create_item_source

# # @pytest.fixture(scope="function")
# # def create_test_score(db_session):    
# #     async def _create_score(item, plugin, score):
# #         score = models.ItemScore(
# #             item_id=item.id,
# #             plugin_id=plugin.id,
# #             score=score
# #         )
# #         db_session.add(score)
# #         await db_session.commit()
# #         return score

# #     return _create_score

