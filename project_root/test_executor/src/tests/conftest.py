import os 
import pytest
import redis
import pika
import httpx

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
