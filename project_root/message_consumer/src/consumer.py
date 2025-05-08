import json, pika
from .connections import rabbitmq_params, EXCHANGE_NAME, DLX_QUEUE, ALT_QUEUE
from .utils import date_print

def forward_callback(ch, method, props, body):
    """
    Any message that lands here is a *failure*.
    If correlation-id + reply_to are present, send a negative reply
    back to the waiting client; otherwise just log & ack.
    """
    # --- choose a reason --------------------------------------------------
    if props.headers and "x-death" in props.headers:
        reason  = "Dead Letter"               # TTL expired, rejected, etc.
        detail  = props.headers.get("x-rejection-reason", "")
        rk_orig = props.headers["x-death"][0]["routing-keys"][0]
    else:
        reason  = "Alt Ex"                    # no queue matched
        detail  = f"No consumers for {method.routing_key}"
        rk_orig = method.routing_key

    date_print(f"{reason}: {rk_orig}")

    # --- forward to client if possible -----------------------------------
    if props.reply_to and props.correlation_id:
        failure_payload = json.dumps({
            "valid": False,
            "response_data": None,
            "failure_reason": reason,
            "failure_detail": detail,
        })
        ch.basic_publish(
            exchange="",                      # default direct exchange
            routing_key=props.reply_to,
            body=failure_payload,
            properties=pika.BasicProperties(
                delivery_mode=2,
                correlation_id=props.correlation_id,
            ),
        )
        date_print(f"   → forwarded to {props.reply_to}  corr_id={props.correlation_id}")

    # --- always ACK the DLX copy -----------------------------------------
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_forwarder():
    conn    = pika.BlockingConnection(rabbitmq_params)
    channel = conn.channel()

    channel.basic_qos(prefetch_count=10)

    # only DLX_QUEUE if you drop ALT; add ALT_QUEUE if you keep it
    for q in (DLX_QUEUE, ALT_QUEUE):
        try:
            channel.basic_consume(queue=q, on_message_callback=forward_callback)
        except pika.exceptions.ChannelClosedByBroker:
            # queue may not exist if ALT was removed
            channel = conn.channel()
            continue

    date_print("DLX/ALT forwarder running…")
    return channel, conn


def start_consumer(is_dlx=True):
    assert is_dlx 
    channel, connection = start_forwarder()
    return channel, connection




# import os
# import json
# import pika
# import multiprocessing
# from datetime import datetime

# from .connections import rabbitmq_params, redis_client, EXCHANGE_NAME, RESPONSE_QUEUE, ALT_QUEUE, DLX_QUEUE
# from .utils import date_print

# MESSAGE_TTL = int(os.environ['REDIS_MESSAGE_TTL'])

# def get_dlx_channel():
#     date_print(f"Consumer: Starting connection to Alt Ex {EXCHANGE_NAME}.alt on queue {ALT_QUEUE}")

#     connection = pika.BlockingConnection(rabbitmq_params)
#     channel = connection.channel()
    
#     channel.basic_qos(prefetch_count=1)
#     channel.basic_consume(queue=ALT_QUEUE, on_message_callback=dlx_callback)
#     channel.basic_consume(queue=DLX_QUEUE, on_message_callback=dlx_callback)
#     return channel, connection, 'Alt/Dead'

# def dlx_callback(ch, method, properties, body):    
#     if properties.headers and 'x-death' in properties.headers:
#         original_routing_key = properties.headers['x-death'][0]['routing-keys'][0]
#         request_key = original_routing_key
#         failure_reason = 'Dead Letter'
#         failure_detail = properties.headers.get('x-rejection-reason', '')
#     else:
#         request_key = method.routing_key
#         failure_reason = 'Alt Ex'
#         failure_detail = f'No valid consumers for routing key {method.routing_key}'

#     redis_key = request_key.replace('.', ':').replace('request', 'response')
#     date_print(f"{failure_reason} {request_key} -> {redis_key}")

#     response_data = {'valid': False, 
#                      'response_data': None, 
#                      'failure_reason': failure_reason,
#                      'failure_detail' : failure_detail
#                      }
#     redis_client.setex(redis_key, MESSAGE_TTL, json.dumps(response_data))

#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def get_response_channel():
#     date_print(f"Consumer: Starting connection to {EXCHANGE_NAME} on queue {RESPONSE_QUEUE}")
#     connection = pika.BlockingConnection(rabbitmq_params)
#     channel = connection.channel()

#     channel.basic_qos(prefetch_count=1)
#     channel.basic_consume(queue=RESPONSE_QUEUE, on_message_callback=response_callback)
#     return channel, connection, 'Internal'

# def response_callback(ch, method, properties, body):
#     response_data = json.loads(body)

#     redis_key = method.routing_key.replace('.', ':')
#     date_print(f"{method.routing_key} -> {redis_key}")

#     response_data = {'valid' : True, 'response_data' : response_data}
#     redis_client.setex(redis_key, MESSAGE_TTL, json.dumps(response_data))

#     ch.basic_ack(delivery_tag=method.delivery_tag)


# def start_consumer(is_dlx=False):
#     if is_dlx:
#         channel, connection, consumer_type = get_dlx_channel()
#     else:
#         channel, connection, consumer_type = get_response_channel()

#     date_print(f"Consumer: {consumer_type} consumer started. Waiting for messages...")
#     return channel, connection



