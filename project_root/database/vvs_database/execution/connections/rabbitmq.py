import pika, json
import asyncio 
from itertools import islice
from typing import List, Iterable, Any

from vvs_database.schemas import ExecuteRequestUnion, RabbitMQConnection
from vvs_database import logging

class RabbitMQService():
    def __init__(self, 
                 rabbitmq_connection: RabbitMQConnection,
                 verbose: bool=False):
        self.init(rabbitmq_connection)
        self.verbose = verbose
        self._replies: dict[str, Any] = {}

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

    def _on_response(self, ch, method, props, body):
        # self._replies[props.correlation_id] = json.loads(body)
        response = json.loads(body)
        if "failure_reason" not in response:
            # response will have failure reason if it comes from the dlx queue
            response = {
                "valid": True, 
                "response_data": response,
                "failure_reason": None,
                "failure_detail": None
            }
        self._replies[props.correlation_id] = response

    def _on_return(self, ch, method, props, body):
        self._replies[props.correlation_id] = {
            "valid": False,
            "response_data": None,
            "failure_reason": "Unroutable",
            "failure_detail": f"routing_key='{method.routing_key}'",
        }

    def pop_replies(self, ids: list[str]) -> dict[str, Any]:
        return {i: self._replies.pop(i) for i in ids if i in self._replies}
    
    def poll_events(self):
        if self.connection and self.connection.is_open:
            self.connection.process_data_events(time_limit=0.1)

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

            if not getattr(self, "callback_queue", None):
                result = self.channel.queue_declare(queue="", exclusive=True)
                self.callback_queue = result.method.queue
                self.channel.basic_consume(
                    queue=self.callback_queue,
                    on_message_callback=self._on_response,
                    auto_ack=True,
                )
                self.channel.add_on_return_callback(self._on_return)

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
                    mandatory=True,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        reply_to=self.callback_queue,
                        correlation_id=request_id
                        )
                )
                successful_ids.append(request_id)
                if self.verbose:
                    logging.info(f"{self.log_id}: Message published to {request_id}")
            return successful_ids
        
        except pika.exceptions.AMQPError as e:
            logging.error(f"{self.log_id}: Error publishing message: {e}")
            return successful_ids
