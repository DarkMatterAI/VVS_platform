import pika
import json
from sqlalchemy import create_engine
from functools import partial 

from .connections import EXCHANGE_NAME, DB_URL, rabbitmq_params, get_plugin_from_routing_key
from .chem import execute_plugin
from .utils import date_print

def callback(ch, method, properties, body, engine):

    if properties.headers and ('x-rejection-reason' in properties.headers):
        date_print('Message previously rejected - sending to dlx')
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return 

    date_print(f"Received message with routing key {method.routing_key}")

    message_data = json.loads(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    response, valid, reason = execute_plugin(engine, message_data, method.routing_key)
    date_print(f"{method.routing_key} {response} {valid}")

    if valid:
        return_key = method.routing_key.replace('request', 'response')
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=return_key,
            body=json.dumps(response),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        date_print(f"Response published with routing key: {return_key}")
    else:
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=method.routing_key,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2, 
                                            headers={'x-rejection-reason': reason})
        )
        date_print(f"Response rejected for reason: {reason}")

def start_consumer():
    engine = create_engine(DB_URL)
    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()

    result = channel.queue_declare(queue='vvs_rdkit_plugin', durable=True, auto_delete=True, arguments={
        "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx"
    })
    queue_name = result.method.queue

    binding_key = "request.rdkit_plugin.*.*.*.*"

    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key=binding_key
    )

    callback_partial = partial(callback, engine=engine)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback_partial
    )

    date_print(f"Consumer started. Waiting for messages...")
    return channel, connection, engine 
