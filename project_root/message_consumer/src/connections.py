import os
import pika
import time 
import redis

from .utils import date_print

MESSAGE_TTL = int(os.environ['REDIS_MESSAGE_TTL'])
EXCHANGE_NAME = os.environ['RABBITMQ_EXCHANGE_NAME']
RESPONSE_QUEUE = os.environ.get('RABBITMQ_RESPONSE_QUEUE_NAME')
ALT_QUEUE = os.environ.get('RABBITMQ_ALT_QUEUE_NAME')
DLX_QUEUE = os.environ.get('RABBITMQ_DLQ_NAME')

rabbitmq_params = pika.ConnectionParameters(
    host='rabbitmq',
    port=int(os.getenv('RABBITMQ_PORT', 5672)),
    credentials=pika.PlainCredentials(
        os.getenv('RABBITMQ_DEFAULT_USER'),
        os.getenv('RABBITMQ_DEFAULT_PASS')
    )
)

redis_client = redis.Redis(
    host='redis',
    port=int(os.getenv('REDIS_PORT', 6379)),
    password=os.getenv('REDIS_PASSWORD'),
    db=int(os.getenv('REDIS_DB', 0))
)

def create_dlx(channel):
    channel.exchange_declare(exchange=f"{EXCHANGE_NAME}.dlx", exchange_type='fanout')
    channel.queue_declare(DLX_QUEUE)
    channel.queue_bind(exchange=f"{EXCHANGE_NAME}.dlx", queue=DLX_QUEUE)

def create_alt(channel):
    channel.exchange_declare(exchange=f"{EXCHANGE_NAME}.alt", exchange_type='fanout')
    channel.queue_declare(ALT_QUEUE)
    channel.queue_bind(exchange=f"{EXCHANGE_NAME}.alt", queue=ALT_QUEUE)

def create_exchange(channel):
    args = {
        "alternate-exchange": f"{EXCHANGE_NAME}.alt",
        "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx"
    }
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', arguments=args)

    # bind response queue 
    channel.queue_declare(queue=RESPONSE_QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx"
    })

    binding_key = "response.*.*.*.*.*"
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=RESPONSE_QUEUE, routing_key=binding_key)

def setup_rabbitmq():
    while True:
        try:
            date_print('Setting up RabbitMQ')
            connection = pika.BlockingConnection(rabbitmq_params)
            channel = connection.channel()

            create_dlx(channel)
            create_alt(channel)
            create_exchange(channel)

            # close connection 
            channel.close() 
            connection.close()
            date_print('RabbitMQ setup complete')
            return 
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection was closed, retrying... Error: {e}")
            time.sleep(1)
        except Exception as e:
            date_print(f"Unexpected error occurred: {e}")
            return 
        
