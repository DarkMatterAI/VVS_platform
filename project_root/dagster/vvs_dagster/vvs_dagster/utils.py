import dagster as dg 

from vvs_dagster.resources import (
    PostgresResource, 
    RabbitMQResource, 
    RedisResource,
)

from vvs_database.execution.connections import Connections

def get_logger(context: dg.OpExecutionContext):
    from vvs_database import logging 
    logging.set_logger(context.log)
    return logging

def get_connection(postgres_resource: PostgresResource,
                   rabbitmq_resource: RabbitMQResource,
                   redis_resource: RedisResource):
    db_service = postgres_resource.get_service()
    redis_service = redis_resource.get_service()
    rabbitmq_service = rabbitmq_resource.get_service()
    return Connections(db_service=db_service,
                       redis_service=redis_service,
                       rabbitmq_service=rabbitmq_service)