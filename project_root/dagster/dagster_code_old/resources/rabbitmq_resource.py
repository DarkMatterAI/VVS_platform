
import os
from dagster import resource, ConfigurableResource, InitResourceContext
from pydantic import Field
import pika
from pika import PlainCredentials

class RabbitMQResourceConfig(ConfigurableResource):
    host: str = Field(default="rabbitmq")
    port: int = Field(default=5672)
    username: str = Field(default=os.environ.get('RABBITMQ_DEFAULT_USER', ''))
    password: str = Field(default=os.environ.get('RABBITMQ_DEFAULT_PASS', ''))
    exchange_name: str = Field(default=os.environ.get('RABBITMQ_EXCHANGE_NAME', 'vvs_exchange'))
    expiration: str = Field(default=os.environ.get('RABBITMQ_DEFAULT_MESSAGE_EXP', '86400000'))
    delivery_mode: int = Field(default=2)

@resource(config_schema=RabbitMQResourceConfig.to_config_schema())
def rabbitmq_resource(context: InitResourceContext):
    config = RabbitMQResourceConfig.from_resource_context(context)
    credentials = PlainCredentials(config.username, config.password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config.host,
            port=config.port, 
            credentials=credentials
        )
    )
    resource_data = {
        'connection' : connection,
        'exchange' : config.exchange_name,
        'expiration' : config.expiration,
        'delivery_mode' : config.delivery_mode 
    }
    try:
        yield resource_data
    finally:
        connection.close()
