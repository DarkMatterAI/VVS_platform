import pika, json, uuid
import asyncio 
from itertools import islice
from typing import List, Iterable, Any

from vvs_database.schemas import ExecuteRequestUnion
from vvs_database.execution.connections.connection_schemas import RabbitMQConnection
from vvs_database import logging

class RabbitMQService():
    def __init__(self, 
                 rabbitmq_connection: RabbitMQConnection,
                 verbose: bool=False):
        self.init(rabbitmq_connection)
        self.verbose = verbose
        self._replies: dict[str, Any] = {}
        self._batch_map: dict[str, list[str]] = {}

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
        cid  = props.correlation_id
        data = json.loads(body)

        # ── 1. Is this a batched reply? ────────────────────────────────
        if cid in self._batch_map:
            req_ids = self._batch_map.pop(cid)
            # normalise to list for positional pairing
            payloads = data if isinstance(data, list) else [data] * len(req_ids)

            for rid, payload in zip(req_ids, payloads):
                if "failure_reason" not in payload:
                    payload = {
                        "valid": True,
                        "response_data": payload,
                        "failure_reason": None,
                        "failure_detail": None,
                    }
                self._replies[rid] = payload
            return

        # ── 2. Single-message reply ───────────────────────────────────
        if "failure_reason" not in data:
            data = {
                "valid": True,
                "response_data": data,
                "failure_reason": None,
                "failure_detail": None,
            }
        self._replies[cid] = data

    def _on_return(self, ch, method, props, body):
        cid = props.correlation_id

        # batch failure → mark every member
        if cid in self._batch_map:
            req_ids = self._batch_map.pop(cid)
            for rid in req_ids:
                self._replies[rid] = {
                    "valid": False,
                    "response_data": None,
                    "failure_reason": "Unroutable",
                    "failure_detail": f"routing_key='{method.routing_key}'",
                }
            return

        # single message
        self._replies[cid] = {
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


    def publish_messages(self, messages: List[ExecuteRequestUnion], batch_size: int=1) -> List[str]:
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

            if batch_size == 1:
                for message in messages:
                    req_id = message.request_data.request_id
                    self._send_single(message, req_id)
                    successful_ids.append(req_id)
            else:
                # assume `messages` length ≤ batch_size (caller already chunked)
                batch_id  = str(uuid.uuid4())
                req_ids   = [m.request_data.request_id for m in messages]
                self._batch_map[batch_id] = req_ids

                payload = [m.model_dump() for m in messages]
                self.channel.basic_publish(
                    exchange=self.rabbitmq_connection.exchange,
                    routing_key=req_ids[0],           # any routing-key in batch
                    body=json.dumps(payload),
                    mandatory=True,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        reply_to=self.callback_queue,
                        correlation_id=batch_id,
                    ),
                )
                successful_ids.extend(req_ids)

            return successful_ids
        
        except pika.exceptions.AMQPError as e:
            logging.error(f"{self.log_id}: Error publishing message: {e}")
            return successful_ids

    def _send_single(self, message: ExecuteRequestUnion, req_id: str) -> None:
        self.channel.basic_publish(
            exchange=self.rabbitmq_connection.exchange,
            routing_key=req_id,
            body=json.dumps(message.model_dump()),
            mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode=2,
                reply_to=self.callback_queue,
                correlation_id=req_id,
            ),
        )
