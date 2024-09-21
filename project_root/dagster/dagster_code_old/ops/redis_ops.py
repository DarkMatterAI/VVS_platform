from dagster import op, In, Out, Nothing, Config
from pydantic import Field
import time
import os 
import json 

def redis_get_helper(context, redis_client, redis_key):
    context.log.info(f"Redis Get {redis_key}")
    return redis_client.get(redis_key)

def redis_delete_helper(context, redis_client, redis_key):
    context.log.info(f"Redis Delete {redis_key}")
    redis_client.delete(redis_key)

def redis_parse_result(context, result, redis_key):
    try:
        decoded_result = result.decode('utf-8')
        parsed_result = json.loads(decoded_result)
        return {'result': parsed_result}
    except json.JSONDecodeError:
        context.log.error(f"Failed to parse JSON for key {redis_key} - {decoded_result}")
        return {'parse_error': decoded_result}

def redis_get_and_parse(context, redis_client, redis_key, delete):
    result = redis_get_helper(context, redis_client, redis_key)
    if result is not None:
        parsed_result = redis_parse_result(context, result, redis_key)
        if delete and ('result' in parsed_result):
            redis_delete_helper(context, redis_client, redis_key)
        return parsed_result
    return {'not_found': None}

@op(
    ins={'redis_key': In(str), 'delete': In(bool)},
    out={
        'result': Out(dict, is_required=False),
        'not_found': Out(Nothing, is_required=False),
        'parse_error': Out(str, is_required=False)
    },
    required_resource_keys={"redis"}
)
def redis_get(context, redis_key: str, delete: bool):
    redis_client = context.resources.redis
    return redis_get_and_parse(context, redis_client, redis_key, delete)

@op(
    ins={
        'redis_key': In(str),
        'timeout': In(int),
        'delete': In(bool)
    },
    out={
        'result': Out(dict, is_required=False),
        'not_found': Out(Nothing, is_required=False),
        'parse_error': Out(str, is_required=False)
    },
    required_resource_keys={"redis"},
)
def redis_poll(context, redis_key: str, timeout: int, interval: int, delete: bool):
    redis_client = context.resources.redis
    end_time = time.time() + timeout

    while time.time() < end_time:
        result = redis_get_and_parse(context, redis_client, redis_key, delete)
        if 'result' in result or 'parse_error' in result:
            return result
        time.sleep(interval)
    return {'not_found': None}


