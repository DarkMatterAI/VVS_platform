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


    # while time.time() - start_time < timeout:
    #     response = redis_connection.get(response_key)
    #     if response:
    #         redis_connection.delete(response_key)
    #         return json.loads(response)
        
    #     time.sleep(interval)

    # raise TimeoutError(f"No response received for key {response_key} after {timeout} seconds")



# def rabbitmq_publish(channel, routing_key, message):
#     channel.basic_publish(
#         exchange=os.environ['RABBITMQ_EXCHANGE_NAME'], 
#         routing_key=routing_key, 
#         body=json.dumps(message)
#         )


# def publish_and_poll(redis_connection, rabbitmq_connection, 
#                      request_key, request_data, 
#                      interval=0.1, timeout=5):
#     print(f"Publishing message with {request_key}")
#     rabbitmq_publish(rabbitmq_connection, request_key, request_data)
#     response_key = request_id_to_response_key(request_key)
#     response_data = poll_redis(redis_connection, response_key, interval, timeout)
#     return response_data 



    # def _publish_messages(self, messages: List[ExecuteRequestUnion]) -> List[str]:
    #     """
    #     Publish messages to RabbitMQ and return successful request IDs
        
    #     Args:
    #         messages: List of message objects to publish
            
    #     Returns:
    #         List of successfully published request IDs
    #     """
    #     successful_ids = []
    #     try:
    #         if not self.connection or not self.connection.is_open:
    #             self.init_rabbitmq_connection()

    #         for message in messages:
    #             request_id = message.request_data.request_id
    #             message_json = json.dumps(message.model_dump())

    #             self.channel.basic_publish(
    #                 exchange=settings.RABBITMQ_EXCHANGE_NAME,
    #                 routing_key=request_id,
    #                 body=message_json,
    #                 properties=pika.BasicProperties(delivery_mode=2)
    #             )
    #             successful_ids.append(request_id)
    #             print(f"{self.log_id}: Message published to {request_id}")
    #         return successful_ids
        
    #     except pika.exceptions.AMQPError as e:
    #         print(f"{self.log_id}: Error publishing message: {e}")
    #         return successful_ids