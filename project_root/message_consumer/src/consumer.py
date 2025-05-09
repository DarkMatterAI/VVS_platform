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

def start_consumer():
    conn    = pika.BlockingConnection(rabbitmq_params)
    channel = conn.channel()

    channel.basic_qos(prefetch_count=10)

    for q in (DLX_QUEUE, ALT_QUEUE):
        try:
            channel.basic_consume(queue=q, on_message_callback=forward_callback)
        except pika.exceptions.ChannelClosedByBroker:
            # queue may not exist if ALT was removed
            channel = conn.channel()
            continue

    date_print("DLX/ALT forwarder running…")
    return channel, conn

