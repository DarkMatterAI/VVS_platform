from dagster import job, op, In, Out, RunConfig, graph, OpExecutionContext, Optional, FilesystemIOManager
from dagster_docker import docker_executor
from typing import Dict, Any

from ..resources import rabbitmq_resource, redis_resource
from ..ops.rabbitmq_ops import rabbitmq_publish
from ..ops.redis_ops import redis_poll
from ..utils import get_request_key

def get_message(input_id):
    return {'test_message' : f'Test Message {input_id}'} 

def get_test_routing_key(input_id):
    return get_request_key('test', 'test', 'test', input_id) 

@op(out={'message': Out(dict), 'routing_key': Out(str)})
def prepare_message(context: OpExecutionContext):
    input_id = context.op_config['input_id']
    context.log.info(f"creating message")
    message = get_message(input_id)
    routing_key = get_test_routing_key(input_id)
    return {'message': message, 'routing_key': routing_key}

@op(
    ins={
        'result': In(Optional[dict]),
    },
    out=None
)
def process_result(context, result):
    if result is not None:
        context.log.info(f"Received result: {result}")
    else:
        context.log.info(f"Result missing")

@op(out={'timeout':Out(int), 'interval':Out(int)})
def param_wrapper(context):
    return {'timeout' : context.op_config['timeout'], 'interval' : context.op_config['interval']}

@graph
def rabbitmq_redis_test_graph():
    message, routing_key = prepare_message()
    timeout, interval = param_wrapper()
    redis_key = rabbitmq_publish(
        routing_key=routing_key,
        message=message
    )
    result, not_found, parse_error = redis_poll(
        redis_key=redis_key,
        timeout=timeout,
        interval=interval
    )
    process_result(result=result)


# job_config_schema = {
#     "input_id": Field(int, default_value=1, is_required=False),
#     "timeout": Field(int, default_value=60, is_required=False),
#     "interval": Field(int, default_value=5, is_required=False),
# }


@job(
    resource_defs={
        "rabbitmq": rabbitmq_resource,
        "redis": redis_resource
    },
)
def rabbitmq_redis_test_job():
    rabbitmq_redis_test_graph()

@job(
    resource_defs={
        "rabbitmq": rabbitmq_resource,
        "redis": redis_resource,
        "io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")
    },
    executor_def=docker_executor,
    # config=job_config_schema
)
def rabbitmq_redis_test_job_docker():
    rabbitmq_redis_test_graph()
