import os 
import pika 
import json 
import time 

def rabbitmq_publish(channel, messages):
    successful_ids = []
    for message in messages:
        request_id = message.request_data.request_id
        message_json = json.dumps(message.model_dump())

        channel.basic_publish(
            exchange=os.environ['RABBITMQ_EXCHANGE_NAME'],
            routing_key=request_id,
            body=message_json,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        successful_ids.append(request_id)
    return successful_ids 

def poll_redis(redis_connection, response_keys, interval=0.1, timeout=10):
    responses = [None for i in response_keys]
    key_iter = [(i, key) for i,key in enumerate(response_keys)]
    start_time = time.time()

    while (time.time() - start_time < timeout) and key_iter:
        key_iter_next = []
        for (idx, key) in key_iter:
            response = redis_connection.get(key)
            if response:
                responses[idx] = json.loads(response) 
                redis_connection.delete(key)
            else:
                key_iter_next.append((idx, key))
        key_iter = key_iter_next 
        if key_iter:
            time.sleep(interval)

    for i, response in enumerate(responses):
        if response is None:
            raise TimeoutError(f"No response received for key {response_keys[i]} after {timeout} seconds")
        
    return responses 
