import os 
import pytest
import redis
import pika
import httpx
# import asyncio 
# import pytest_asyncio

# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
# from sqlalchemy.orm import sessionmaker

# from vvs_database import Base, settings, schemas, crud
# from vvs_database.testing import create_test_database_url, drop_test_database

# @pytest.fixture(scope="session")
# def event_loop():
#     loop = asyncio.get_event_loop_policy().new_event_loop()
#     yield loop
#     loop.close()

# @pytest_asyncio.fixture(scope="session")
# async def test_engine():
#     # Create test database
#     test_db_url = await create_test_database_url(
#         settings.DEFAULT_DB_URL,
#         settings.POSTGRES_DB_TEST
#     )
    
#     # Create test engine
#     test_engine = create_async_engine(test_db_url)
    
#     async with test_engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)

#     yield test_engine

#     # Cleanup
#     await test_engine.dispose()
    
#     # Drop test database
#     await drop_test_database(
#         settings.DEFAULT_DB_URL,
#         settings.POSTGRES_DB_TEST
#     )


# @pytest_asyncio.fixture(scope="function")
# async def db_session(test_engine):
#     TestingSessionLocal = sessionmaker(
#         bind=test_engine,
#         class_=AsyncSession,
#         expire_on_commit=False,
#         autocommit=False,
#         autoflush=False,
#     )

#     async with TestingSessionLocal() as session:
#         try:
#             yield session
#         finally:
#             await session.rollback()
#             await session.close()

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

