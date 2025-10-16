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
from tests.utils.backend_utils import backend_delete_plugin

@pytest_asyncio.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    # from vvs_database import settings
    from vvs_database.settings import settings 
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

@pytest_asyncio.fixture(scope="function")
async def redis_service():
    from vvs_database.execution.connections.redis import RedisService, RedisConnection
    redis_service = RedisService(RedisConnection(), verbose=True)
    redis_service.init_redis_connection()
    yield redis_service
    await redis_service.close()

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
    yield connection, channel
    channel.close()
    connection.close()

@pytest.fixture(scope="session")
def backend_client():
    with httpx.Client(base_url=f"http://backend:{os.environ['BACKEND_PORT']}") as client:
        yield client

@pytest.fixture(scope="function")
def plugin_cleanup(backend_client):
    embedding_records = []
    other_records = []
    def _add_record(record):
        if record['type'] == 'embedding':
            embedding_records.append(record)
        else:
            other_records.append(record)
    yield _add_record 

    # delete embeddings last
    for record_list in [other_records, embedding_records]:
        for record in record_list:
            backend_delete_plugin(backend_client, '/api/v1/plugins', record, ignore_404=True)

@pytest.fixture(scope="function")
def job_cleanup(backend_client):
    job_records = []
    def _add_record(record):
        job_records.append(record)
    yield _add_record 

    for record in job_records:
        backend_delete_plugin(backend_client, '/api/v1/jobs', record, ignore_404=True)

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
    local_path = '/code/test_files'
    def _upload_test_files(filename: str):
        if not filename.startswith(local_path):
            filename = f"{local_path}/{filename}"
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

