from sqlalchemy.orm import class_mapper
from app import models, schemas
import httpx 
from fastapi import HTTPException
import uuid 
import redis
import pika
import os 
import json 
import yaml
import time 
import asyncio

from app.crud.qdrant_utils import qdrant_query


plugin_type_map = {
    schemas.PluginType.EMBEDDING : {
        'create_model' : schemas.EmbeddingPluginCreate,
        'response_model' : schemas.EmbeddingPluginInDB,
        'data_model' : models.EmbeddingPlugin,
        'execute_request_model' : schemas.EmbedRequest
    },
    schemas.PluginType.DATA_SOURCE : {
        'create_model' : schemas.DataSourcePluginCreate,
        'response_model' : schemas.DataSourcePluginInDB,
        'data_model' : models.DataSourcePlugin,
        'execute_request_model' : schemas.DataSourceRequest
    },
    schemas.PluginType.FILTER : {
        'create_model' : schemas.FilterPluginCreate,
        'response_model' : schemas.FilterPluginInDB,
        'data_model' : models.FilterPlugin,
        'execute_request_model' : schemas.ItemRequest
    },
    schemas.PluginType.SCORE : {
        'create_model' : schemas.ScorePluginCreate,
        'response_model' : schemas.ScorePluginInDB,
        'data_model' : models.ScorePlugin,
        'execute_request_model' : schemas.ItemRequest
    },
    schemas.PluginType.MAPPER : {
        'create_model' : schemas.MapperPluginCreate,
        'response_model' : schemas.MapperPluginInDB,
        'data_model' : models.MapperPlugin,
        'execute_request_model' : schemas.MapperRequest
    },
    schemas.PluginType.ASSEMBLY : {
        'create_model' : schemas.AssemblyPluginCreate,
        'response_model' : schemas.AssemblyPluginInDB,
        'data_model' : models.AssemblyPlugin,
        'execute_request_model' : schemas.AssemblyRequest
    }
}

def read_config():
    with open('app/launch_config.yaml', 'r') as file:
        return yaml.safe_load(file)

def object_as_dict(obj):
    output = {}
    for c in class_mapper(obj.__class__).columns:
        output[c.key] = getattr(obj, c.key)
    return output 

def get_plugin_create(plugin_type: schemas.PluginType) -> schemas.PluginBase:
    return plugin_type_map[plugin_type]['create_model']

def get_plugin_data_model(plugin_type: schemas.PluginType) -> schemas.PluginBase:
    return plugin_type_map[plugin_type]['data_model']

def remap_embeddings(plugin: models.Plugin, plugin_dict: dict) -> None:

    if isinstance(plugin, models.DataSourcePlugin):
        plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings]
    elif isinstance(plugin, (models.FilterPlugin, models.ScorePlugin)):
        plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings] if plugin.embeddings else None
    elif isinstance(plugin, models.MapperPlugin):
        plugin_dict['input_embedding_id'] = plugin.input_embedding_id
        plugin_dict['output_embedding_ids'] = [e.id for e in plugin.output_embeddings]

def get_plugin_response_model(plugin: models.Plugin) -> schemas.PluginInDBUnion:
    plugin_dict = object_as_dict(plugin)
    remap_embeddings(plugin, plugin_dict)

    return plugin_type_map[plugin.type]['response_model'].model_validate(plugin_dict)

def validate_updates(plugin: models.Plugin, update_data: dict):

    plugin_dict = object_as_dict(plugin)
    remap_embeddings(plugin, plugin_dict)
    plugin_dict.update(update_data)
    plugin_type_map[plugin.type]['response_model'].model_validate(plugin_dict)

def validate_execute_request(plugin: models.Plugin, execute_request: dict):
    print(f"Validating {execute_request}, {plugin.type}")
    plugin_type_map[plugin.type]['execute_request_model'].model_validate(execute_request)

def get_request_key(plugin: models.Plugin, item_id=None):
    group_key = plugin.group_key 
    plugin_type = plugin.type 
    plugin_id = plugin.id  
    request_id = uuid.uuid4()

    if item_id is None:
        item_id = uuid.uuid4()

    request_key = f"request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}"
    return request_key 

async def make_post_request(url, data, timeout, retries, retry_sleep=0):
    async with httpx.AsyncClient() as client:
        for attempt in range(retries + 1):
            print(f"Post Request to {url} attempt {attempt+1}")
            try:
                response = await client.post(url, json=data, timeout=timeout )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if attempt == retries:
                    raise HTTPException(status_code=e.response.status_code, detail=str(e))
            except httpx.RequestError as e:
                if attempt == retries:
                    raise HTTPException(status_code=500, detail=f"An error occurred while requesting the API: {str(e)}")
            except Exception as e:
                if attempt == retries:
                    raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
                
            if retry_sleep>0:
                print(f"Post request failed, sleeping")
                time.sleep(retry_sleep)

    raise HTTPException(status_code=500, detail="Failed to execute API plugin after maximum retries")

def get_redis_result(result_id, delete=True):
    redis_client = redis.Redis(host='redis', 
                               port=os.environ['REDIS_PORT'],
                               password=os.environ['REDIS_PASSWORD'],
                               db=int(os.environ['REDIS_DB'])
                               ) 

    redis_key = result_id.replace('.', ':')
    result = redis_client.get(redis_key)

    if not result:
        return {'result_id' : result_id}
    else:
        try:
            decoded_result = result.decode('utf-8')
            parsed_result = json.loads(decoded_result)
            if delete:
                redis_client.delete(redis_key)
            return parsed_result 
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Invalid JSON data in Redis")

async def execute_api_plugin(plugin: models.Plugin, execute_request: dict):
    url = plugin.endpoint_url
    timeout = plugin.timeout
    retries = plugin.max_retries
    execute_request = execute_request.model_dump()
    execute_request['request_id'] = get_request_key(plugin, execute_request.get('id', None))
    response = await make_post_request(url, execute_request, timeout, retries)
    return response 

async def execute_tei_plugin(plugin: models.Plugin, execute_request: dict):
    url = plugin.endpoint_url
    timeout = plugin.timeout
    retries = plugin.max_retries
    execute_request = execute_request.model_dump()
    data = {'inputs' : execute_request.get('item', '')}
    data.update(plugin.config)

    response = await make_post_request(url, data, timeout, retries)
    response = {'embedding' : response[0]}
    return response 

def rabbitmq_publish(routing_key, message):
    rabbitmq_params = pika.ConnectionParameters(
        host='rabbitmq',
        port=int(os.environ.get('RABBITMQ_PORT', 5672)),
        credentials=pika.PlainCredentials(
            os.environ['RABBITMQ_DEFAULT_USER'],
            os.environ['RABBITMQ_DEFAULT_PASS']
        )
    )

    try:
        connection = pika.BlockingConnection(rabbitmq_params)
        channel = connection.channel()
        
        channel.basic_publish(
            exchange=os.environ['RABBITMQ_EXCHANGE_NAME'],
            routing_key=routing_key,
            body=json.dumps(message)
        )
        
        print(f"Message published to {routing_key}")
    except pika.exceptions.AMQPError as e:
        print(f"Error publishing message: {e}")
    finally:
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()

async def execute_queue_plugin(plugin: models.Plugin, execute_request: dict):

    execute_request = execute_request.model_dump()
    request_key = get_request_key(plugin, execute_request.get('id', None))
    execute_request['request_id'] = request_key
    rabbitmq_publish(request_key, execute_request)
    response_key = request_key.replace('request', 'response')
    await asyncio.sleep(0)
    return {'result_id' : response_key}

async def execute_qdrant_plugin(plugin: models.Plugin, execute_request: dict):
    execute_request = execute_request.model_dump()
    try:
        response = await qdrant_query(plugin, execute_request)
        return response 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while querying qdrant: {str(e)}")


execute_plugin_map = {
    'api' : execute_api_plugin,
    'queue' : execute_queue_plugin,
    'internal_tei' : execute_tei_plugin,
    'internal_qdrant' : execute_qdrant_plugin
}

