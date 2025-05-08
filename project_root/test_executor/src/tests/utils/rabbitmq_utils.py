import uuid, json, os, pika, time

def rabbitmq_publish(channel, messages, reply_queue):
    """
    Publish a batch of RPC requests.  
    Returns the list of correlation-ids in the same order as *messages*.
    """
    corr_ids = []
    for msg in messages:
        corr_id = uuid.uuid4().hex          # unique per request
        corr_ids.append(corr_id)

        channel.basic_publish(
            exchange=os.environ["RABBITMQ_EXCHANGE_NAME"],
            routing_key=msg.request_data.request_id,
            body=json.dumps(msg.model_dump()),
            properties=pika.BasicProperties(
                delivery_mode=2,
                reply_to=reply_queue,
                correlation_id=corr_id,
            ),
        )
    return corr_ids

def collect_replies(connection: pika.BlockingConnection,
                    channel:     pika.channel.Channel,
                    reply_queue: str,
                    corr_ids:    list[str],
                    interval:    float = 0.05,
                    timeout:     float = 10.0) -> list[dict]:
    """
    Block until we have a reply for every correlation-id, or raise TimeoutError.
    Returns the list of *response payloads* in the same order as corr_ids.
    """
    waiting  = set(corr_ids)
    replies  = {c: None for c in corr_ids}
    start    = time.time()

    while waiting and (time.time() - start < timeout):
        # let pika dispatch any buffered frames without blocking
        connection.process_data_events(time_limit=0)

        method, props, body = channel.basic_get(queue=reply_queue, auto_ack=True)
        if method:          # got a message
            c = props.correlation_id
            if c in waiting:
                replies[c] = json.loads(body)
                waiting.remove(c)
        else:
            time.sleep(interval)

    if waiting:
        missing = ", ".join(waiting)
        raise TimeoutError(f"Timeout waiting for replies: {missing}")

    # return in original publish order
    return [replies[c] for c in corr_ids]

