import os 
import pytest
import redis
import pika
import httpx
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

@pytest.fixture(scope="session")
def redis_connection():
    redis_client = redis.Redis(host='redis', 
                               port=os.environ['REDIS_PORT'],
                               password=os.environ['REDIS_PASSWORD'],
                               db=int(os.environ['REDIS_DB'])
                               ) 
    yield redis_client
    redis_client.close()

@pytest.fixture(scope="session")
def rabbitmq_connection():
    rabbitmq_params = pika.ConnectionParameters(
        host='rabbitmq',
        port=int(os.environ.get('RABBITMQ_PORT', 5672)),
        credentials=pika.PlainCredentials(
            os.environ['RABBITMQ_DEFAULT_USER'],
            os.environ['RABBITMQ_DEFAULT_PASS']
        )
    )

    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()
    yield channel 
    channel.close()
    connection.close()

@pytest.fixture(scope="session")
def backend_client():
    with httpx.Client(base_url=f"http://backend:{os.environ['BACKEND_PORT']}") as client:
        yield client

@pytest.fixture(scope="session")
def test_api_client():
    with httpx.Client(base_url=f"http://test_server:{os.environ['TEST_SERVER_PORT']}") as client:
        yield client

@pytest.fixture(scope="session")
def tei_client():
    with httpx.Client(base_url=f"http://tei_plugin:{os.environ['TEI_PORT']}") as client:
        yield client

@pytest.fixture(scope="session")
def triton_client():
    with httpx.Client(base_url=f"http://triton_plugin:{os.environ['TRITON_HTTP_PORT']}") as client:
        yield client

