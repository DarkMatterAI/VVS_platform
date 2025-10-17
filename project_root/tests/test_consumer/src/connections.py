import os
import pika

EXCHANGE_NAME = os.environ['RABBITMQ_EXCHANGE_NAME']

rabbitmq_params = pika.ConnectionParameters(
    host='rabbitmq',
    port=int(os.getenv('RABBITMQ_PORT', 5672)),
    credentials=pika.PlainCredentials(
        os.getenv('RABBITMQ_DEFAULT_USER'),
        os.getenv('RABBITMQ_DEFAULT_PASS')
    )
)