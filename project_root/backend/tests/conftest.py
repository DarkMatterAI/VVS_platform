import asyncio
import pytest
import itertools 
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.main import app
from app.core.database import Base, get_db
from app import models

# Use an in-memory SQLite database for testing
SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def test_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.fixture(scope="function")
async def db_session(test_db):
    async with TestingSessionLocal() as session:
        yield session
        await session.rollback()

@pytest.fixture(scope="function")
async def client(db_session):
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()

@pytest.fixture(scope="function")
def create_test_embedding(db_session):
    counter = itertools.count(1)
    
    async def _create_embedding(name=None, vector_length=128, distance_metric="Cosine"):
        embedding = models.EmbeddingPlugin(
            name=name or f"Test Embedding {next(counter)}",
            type="embedding",
            execution_type="internal",
            group_key="test",
            timeout=30,
            max_concurrency=5,
            max_retries=1,
            vector_length=vector_length,
            distance_metric=distance_metric
        )
        db_session.add(embedding)
        await db_session.commit()
        await db_session.refresh(embedding)
        return embedding

    return _create_embedding
