import pika
import json 
from typing import List 

from vvs_database.settings import settings 
from vvs_database.schemas import ExecuteRequestUnion

class RabbitMQService():
    def __init__(self):
        self.connection = None 
        self.channel = None 
        self.log_id = ''

    def init_rabbitmq_connection(self):
        """Initialize connection to RabbitMQ"""
        if (
            self.channel and 
            self.channel.is_open and 
            self.connection and 
            self.connection.is_open
        ):
            return 
        
        try:
            print(f"{self.log_id}: Trying to connect to RabbitMQ")
            rabbitmq_params = pika.ConnectionParameters(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                credentials=pika.PlainCredentials(
                    settings.RABBITMQ_DEFAULT_USER,
                    settings.RABBITMQ_DEFAULT_PASS
                ),
                heartbeat=30,
                blocked_connection_timeout=10
            )
            self.connection = pika.BlockingConnection(rabbitmq_params)
            self.channel = self.connection.channel()
            print(f"{self.log_id}: Successfully connected to RabbitMQ")
        except Exception as e:
            print(f"{self.log_id}: Failed to connect to RabbitMQ: {str(e)}")
            raise 

    async def close(self):
        """Close RabbitMQ connections"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            print(f"{self.log_id}: Error closing RabbitMQ connections: {str(e)}")

    def publish_messages(self, messages: List[ExecuteRequestUnion]) -> List[str]:
        """
        Publish messages to RabbitMQ and return successful request IDs
        
        Args:
            messages: List of message objects to publish
            
        Returns:
            List of successfully published request IDs
        """
        successful_ids = []
        try:
            if not self.connection or not self.connection.is_open:
                self.init_rabbitmq_connection()

            for message in messages:
                request_id = message.request_data.request_id
                message_json = json.dumps(message.model_dump())

                self.channel.basic_publish(
                    exchange=settings.RABBITMQ_EXCHANGE_NAME,
                    routing_key=request_id,
                    body=message_json,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                successful_ids.append(request_id)
                print(f"{self.log_id}: Message published to {request_id}")
            return successful_ids
        
        except pika.exceptions.AMQPError as e:
            print(f"{self.log_id}: Error publishing message: {e}")
            return successful_ids
