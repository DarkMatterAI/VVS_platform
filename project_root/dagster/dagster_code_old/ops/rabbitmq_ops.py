import json 
import pika 
from dagster import op, In, Out

@op(
        ins={'routing_key' : In(str), 'message' : In(dict)},
        out={'redis_key' : Out(str)},
        required_resource_keys={"rabbitmq"}
    )
def rabbitmq_publish(context, routing_key, message):
    rabbitmq_resource = context.resources.rabbitmq
    connection = rabbitmq_resource['connection']
    exchange = rabbitmq_resource['exchange']

    context.log.info(f"RabbitMQ Publish {exchange} {routing_key}")

    channel = connection.channel()
    channel.basic_publish(exchange=exchange,
                          routing_key=routing_key,
                          body=json.dumps(message),
                          properties=pika.BasicProperties(expiration=rabbitmq_resource['expiration'],
                                                          delivery_mode=rabbitmq_resource['delivery_mode'])
                        )
    channel.close()
    redis_key = routing_key.replace('.', ':').replace('request', 'response')
    return redis_key

