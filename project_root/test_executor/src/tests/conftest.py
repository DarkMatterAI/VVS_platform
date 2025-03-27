import os 
import pytest 
import pytest_asyncio
import httpx 
import redis 
import asyncio
import pika 
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from vvs_database.crud import get_s3_client, upload_file, delete_file, check_file_exists

@pytest_asyncio.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    from vvs_database import settings
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

@pytest.fixture(scope="session")
def s3_client():
    s3_client = get_s3_client()
    yield s3_client
    s3_client.close()

@pytest.fixture(scope="session")
def upload_test_files(s3_client):
    uploaded_files = []
    def _upload_test_files(filename):
        basename = os.path.basename(filename)
        if basename not in uploaded_files:
            with open(filename, 'rb') as file_obj:
                result = upload_file(basename, file_obj, s3_client)
            assert check_file_exists(basename, s3_client)
            uploaded_files.append(basename)
    yield _upload_test_files
    for filename in uploaded_files:
        delete_file(filename, s3_client)
        assert not check_file_exists(filename, s3_client)

