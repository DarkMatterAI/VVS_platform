import pika
import json
from sqlalchemy import create_engine
from functools import partial 

from .connections import EXCHANGE_NAME, DB_URL, rabbitmq_params, get_plugin_from_routing_key
from .chem import execute_plugin
from .utils import date_print

def callback(ch, method, properties, body, engine):
    # ── 1. short‑circuit previously rejected messages ────────────────
    if properties.headers and "x-rejection-reason" in properties.headers:
        date_print("Message previously rejected - sending to DLX")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    date_print(f"Received message {method.routing_key}")

    # ── 2. run the plugin code ───────────────────────────────────────
    payload      = json.loads(body)
    response, valid, reason = execute_plugin(engine, payload, method.routing_key)
    date_print(f"{method.routing_key} valid={valid}")

    # ── 3. on success → RPC reply  ───────────────────────────────────
    if valid and properties.reply_to:
        ch.basic_publish(
            exchange="",                         # default direct exchange
            routing_key=properties.reply_to,     # client’s private queue
            body=json.dumps(response),
            properties=pika.BasicProperties(
                delivery_mode=2,                 # persist
                correlation_id=properties.correlation_id,
            ),
        )
        date_print(
            f"Sent reply corr_id={properties.correlation_id} → {properties.reply_to}"
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # ── 4. on failure (or missing reply_to) → dead‑letter ────────────
    ch.basic_publish(
        exchange=EXCHANGE_NAME,                  # same topic exchange
        routing_key=method.routing_key,          # so DLX rules apply
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={"x-rejection-reason": reason},
        ),
    )
    date_print(f"Rejected request: {reason}")
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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

    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback_partial
    )

    date_print(f"Consumer started. Waiting for messages...")
    return channel, connection, engine 
