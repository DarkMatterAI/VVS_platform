
import pika 

from .connections import EXCHANGE_NAME, rabbitmq_params

def callback(ch, method, properties, body):
    print(f"Received message: {body} - rejecting to Dead Letter")    
    ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    
def start_consumer(worker_id):
    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()

    result = channel.queue_declare(queue='', durable=True, auto_delete=True, arguments={
        "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx"
    })
    queue_name = result.method.queue

    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key='request.*.dlx_test.*.*.*'
    )

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback
    )

    print(f"Worker {worker_id}: DLX Test consumer started for plugins. Waiting for messages...")
    return channel, connection
    # channel.start_consuming()

