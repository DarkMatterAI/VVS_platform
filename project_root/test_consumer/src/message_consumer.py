import pika
import json
import multiprocessing
from datetime import datetime

from .plugin_funcs import func_mapping
from .connections import EXCHANGE_NAME, rabbitmq_params

def execute_plugin(message_data, plugin_type):
    func = func_mapping[plugin_type]
    return func(message_data)

def callback(ch, method, properties, body):
    worker_id = multiprocessing.current_process().name
    response_data = json.loads(body)

    print(f'{str(datetime.now())} - Worker {worker_id}: {method.routing_key}')

    # request.<group_key>.<plugin_type>.<plugin_id>.<item_id>.<request_id>
    _, group_key, plugin_type, plugin_id, item_id, request_id = method.routing_key.split('.')

    return_key = method.routing_key.replace('request', 'response')

    response = execute_plugin(response_data, plugin_type)

    # ──────────────────────────────────────────────────────────────
    #  ┌─►  send *directly* back to the client-side reply queue
    #  │     obtained from   BasicProperties.reply_to
    #  └─►  echo the correlation-id so the client can match the RPC
    #
    runtime_args = response_data.get("runtime_args") or {}
    if not runtime_args.get("no_response", False) and properties.reply_to:
        ch.basic_publish(
            exchange="",                         # default direct-to-queue exchange
            routing_key=properties.reply_to,     # the client’s private queue
            body=json.dumps(response),
            properties=pika.BasicProperties(
                delivery_mode=2,                 # persistent
                correlation_id=properties.correlation_id,
            ),
        )
        print(
            f"Sent reply corr_id={properties.correlation_id} -> {properties.reply_to}"
        )

    ch.basic_ack(delivery_tag=method.delivery_tag)  # always ACK last

def start_consumer(worker_id, records):
    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()

    result = channel.queue_declare(queue='', durable=True, auto_delete=True, arguments={
        "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx"
    })
    queue_name = result.method.queue

    binding_keys = []
    for plugin_type, plugin_records in records.items():
        binding_keys += [f"request.mock_queue.{plugin_type}.{i['id']}.*.*" for i in plugin_records]

    print(binding_keys)

    for binding_key in binding_keys:
        channel.queue_bind(
            exchange=EXCHANGE_NAME,
            queue=queue_name,
            routing_key=binding_key
        )

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback
    )

    print(f"Worker {worker_id}: Test consumer started. Waiting for messages...")
    return channel, connection
