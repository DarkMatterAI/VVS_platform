import pika
import json 
import asyncio 
from typing import List

from vvs_database.schemas import ExecuteRequestUnion, RabbitMQConnection
from vvs_database import logging

class RabbitMQService():
    def __init__(self, 
                 rabbitmq_connection: RabbitMQConnection,
                 verbose: bool=False):
        self.init(rabbitmq_connection)
        self.verbose = verbose

    def init(self, rabbitmq_connection: RabbitMQConnection):
        self.connection = None 
        self.channel = None 
        self.log_id = ''
        self.rabbitmq_connection = rabbitmq_connection
        self.rabbitmq_params = pika.ConnectionParameters(
            host=rabbitmq_connection.host,
            port=rabbitmq_connection.port,
            credentials=pika.PlainCredentials(
                username=rabbitmq_connection.username,
                password=rabbitmq_connection.password
            ),
            heartbeat=120,
            blocked_connection_timeout=10
        )

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
            logging.info(f"{self.log_id}: Trying to connect to RabbitMQ")
            self.connection = pika.BlockingConnection(self.rabbitmq_params)
            self.channel = self.connection.channel()
            logging.info(f"{self.log_id}: Successfully connected to RabbitMQ")
        except Exception as e:
            logging.info(f"{self.log_id}: Failed to connect to RabbitMQ: {str(e)}")
            raise 

    async def close(self):
        """Close RabbitMQ connections"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
            await asyncio.sleep(0)
        except Exception as e:
            logging.error(f"{self.log_id}: Error closing RabbitMQ connections: {str(e)}")

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
                    exchange=self.rabbitmq_connection.exchange,
                    routing_key=request_id,
                    body=message_json,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                successful_ids.append(request_id)
                if self.verbose:
                    logging.info(f"{self.log_id}: Message published to {request_id}")
            return successful_ids
        
        except pika.exceptions.AMQPError as e:
            logging.error(f"{self.log_id}: Error publishing message: {e}")
            return successful_ids
