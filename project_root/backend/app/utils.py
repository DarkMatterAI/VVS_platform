from sqlalchemy.orm import class_mapper
from app import schemas 
import httpx 
from fastapi import HTTPException
import uuid 
import pika
import os 
import json 
import yaml
import time 
import asyncio

from aioredis import Redis
# from app.core.settings import settings 
from vvs_database import settings 
from vvs_database.models import (Plugin, 
                                 EmbeddingPlugin, 
                                 DataSourcePlugin, 
                                 FilterPlugin, 
                                 ScorePlugin, 
                                 MapperPlugin, 
                                 AssemblyPlugin
                                )

from vvs_database.schemas.enums import PluginType, PluginClass
from vvs_database.utils import get_redis_result, get_redis_result_batch, make_post_request

# Plugin type mapping for API layer
# plugin_type_map = {
#     PluginType.EMBEDDING : {
#         'create_model' : schemas.EmbeddingPluginCreate,
#         'response_model' : schemas.EmbeddingPluginInDB,
#         'data_model' : EmbeddingPlugin,
#         'execute_request_model' : schemas.EmbedRequest
#     },
#     PluginType.DATA_SOURCE : {
#         'create_model' : schemas.DataSourcePluginCreate,
#         'response_model' : schemas.DataSourcePluginInDB,
#         'data_model' : DataSourcePlugin,
#         'execute_request_model' : schemas.DataSourceRequest
#     },
#     PluginType.FILTER : {
#         'create_model' : schemas.FilterPluginCreate,
#         'response_model' : schemas.FilterPluginInDB,
#         'data_model' : FilterPlugin,
#         'execute_request_model' : schemas.ItemRequest
#     },
#     PluginType.SCORE : {
#         'create_model' : schemas.ScorePluginCreate,
#         'response_model' : schemas.ScorePluginInDB,
#         'data_model' : ScorePlugin,
#         'execute_request_model' : schemas.ItemRequest
#     },
#     PluginType.MAPPER : {
#         'create_model' : schemas.MapperPluginCreate,
#         'response_model' : schemas.MapperPluginInDB,
#         'data_model' : MapperPlugin,
#         'execute_request_model' : schemas.MapperRequest
#     },
#     PluginType.ASSEMBLY : {
#         'create_model' : schemas.AssemblyPluginCreate,
#         'response_model' : schemas.AssemblyPluginInDB,
#         'data_model' : AssemblyPlugin,
#         'execute_request_model' : schemas.AssemblyRequest
#     }
# }

def read_config():
    """Read application configuration from YAML file."""
    with open('app/launch_config.yaml', 'r') as file:
        return yaml.safe_load(file)
    
async def fastapi_post_request(data, url, timeout, retries, retry_sleep=0):
    """
    A wrapper around make_post_request that converts exceptions to FastAPI HTTPExceptions.
    
    Args:
        data: JSON serializable data to send in the request
        url: The URL to send the request to
        timeout: Timeout in seconds
        retries: Number of retries to attempt
        retry_sleep: Sleep time between retries in seconds
        
    Returns:
        The JSON response from the server
        
    Raises:
        HTTPException: If any error occurs during the request
    """
    try:
        return await make_post_request(data, url, timeout, retries, retry_sleep)
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        raise HTTPException(status_code=status_code, detail=str(e))
    except httpx.TimeoutException as e:
        raise HTTPException(
            status_code=504,  # Gateway Timeout
            detail=str(e)
        )
    except httpx.ConnectError as e:
        raise HTTPException(
            status_code=503,  # Service Unavailable
            detail=str(e)
        )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,  # Internal Server Error
            detail=str(e)
        )
    except Exception as e:
        # Catch-all for any other unexpected exceptions
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )

# def object_as_dict(obj):
#     """Convert SQLAlchemy model instance to dictionary."""
#     output = {}
#     for c in class_mapper(obj.__class__).columns:
#         output[c.key] = getattr(obj, c.key)
#     return output 

# def get_plugin_create(plugin_type: PluginType):
#     """Get the Pydantic model for creating a specific plugin type."""
#     return plugin_type_map[plugin_type]['create_model']

# def get_plugin_data_model(plugin_type: PluginType):
#     """Get the SQLAlchemy model for a specific plugin type."""
#     return plugin_type_map[plugin_type]['data_model']

# def remap_embeddings(plugin: Plugin, plugin_dict: dict) -> None:
#     """Add embedding relationship data to plugin dictionary."""
#     if isinstance(plugin, DataSourcePlugin):
#         plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings]
#     elif isinstance(plugin, (FilterPlugin, ScorePlugin)):
#         plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings] if plugin.embeddings else None
#     elif isinstance(plugin, MapperPlugin):
#         plugin_dict['input_embedding_id'] = plugin.input_embedding_id
#         plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings]

# def get_plugin_response_model(plugin: Plugin):
#     """Convert SQLAlchemy plugin instance to Pydantic response model."""
#     plugin_dict = object_as_dict(plugin)
#     remap_embeddings(plugin, plugin_dict)

#     return plugin_type_map[plugin.type]['response_model'].model_validate(plugin_dict)

# def validate_updates(plugin: Plugin, update_data: dict):
#     """Validate that plugin updates are consistent with the model schema."""
#     plugin_dict = object_as_dict(plugin)
#     remap_embeddings(plugin, plugin_dict)
#     plugin_dict.update(update_data)
#     plugin_type_map[plugin.type]['response_model'].model_validate(plugin_dict)

# def validate_execute_request(plugin: Plugin, execute_request: list[dict]):
#     """Validate that execution request matches plugin requirements."""
#     request_model = plugin_type_map[plugin.type]['execute_request_model']
#     for item in execute_request: 
#         request_model.model_validate(item)

# def get_request_key(plugin: Plugin, item_id=None):
#     """Generate a unique request key for message queue."""
#     group_key = plugin.group_key 
#     plugin_type = plugin.type 
#     plugin_id = plugin.id  
#     request_id = uuid.uuid4()

#     if item_id is None:
#         item_id = uuid.uuid4()

#     request_key = f"request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}"
#     return request_key 

# async def make_post_request(data, url, timeout, retries, retry_sleep=0):
#     """Make HTTP POST request with retry logic."""
#     async with httpx.AsyncClient() as client:
#         for attempt in range(retries + 1):
#             print(f"Post Request to {url} attempt {attempt+1}")
#             try:
#                 response = await client.post(url, json=data, timeout=timeout)
#                 response.raise_for_status()
#                 return response.json()
#             except httpx.HTTPStatusError as e:
#                 if attempt == retries:
#                     raise HTTPException(status_code=e.response.status_code, detail=str(e))
#             except httpx.RequestError as e:
#                 if attempt == retries:
#                     raise HTTPException(status_code=500, detail=f"An error occurred while requesting the API: {str(e)}")
#             except Exception as e:
#                 if attempt == retries:
#                     raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
                
#             if retry_sleep > 0:
#                 print(f"Post request failed, sleeping")
#                 time.sleep(retry_sleep)

#     raise HTTPException(status_code=500, detail="Failed to execute API plugin after maximum retries")

# async def concurrency_bounded_func(semaphore, func, input, kwargs):
#     """Run function within concurrency limit."""
#     async with semaphore:
#         output = await func(input, **kwargs)
#     return output

# async def concurrency_wrapper(concurrency, func, iterable, kwargs):
#     """Run multiple tasks with concurrency limit."""
#     semaphore = asyncio.Semaphore(concurrency)
    
#     tasks = [concurrency_bounded_func(semaphore, func, item, kwargs) for item in iterable]
#     results = await asyncio.gather(*tasks)
#     return results
    
# async def get_redis_result(result_id: str, delete: bool = True):
#     """Get result from Redis by ID."""
#     redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
#     redis_key = result_id.replace('.', ':')
    
#     try:
#         result = await redis.get(redis_key)
#         if not result:
#             return {'result_id': result_id}
        
#         try:
#             parsed_result = json.loads(result)
#             if delete:
#                 await redis.delete(redis_key)
#             return parsed_result
#         except json.JSONDecodeError as e:
#             result = {
#                 'valid': False,
#                 'response_data': None,
#                 'failure_reason': 'Json decode error - Invalid JSON data in Redis result',
#                 'failure_detail': str(e)
#             }
#             raise HTTPException(status_code=500, detail="Invalid JSON data in Redis")
            
#     finally:
#         await redis.close()

# async def get_redis_result_batch(result_ids: list[dict], delete: bool = True):
#     """Get multiple results from Redis."""
#     redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
#     results = []
    
#     try:
#         result_ids = [i.result_id for i in result_ids]
#         redis_keys = [rid.replace('.', ':') for rid in result_ids]
#         raw_results = await redis.mget(redis_keys)
        
#         if delete:
#             # Delete keys where result was found
#             keys_to_delete = [
#                 key for key, result in zip(redis_keys, raw_results)
#                 if result is not None
#             ]
#             if keys_to_delete:
#                 await redis.delete(*keys_to_delete)
        
#         # Process each result
#         for result_id, raw_result in zip(result_ids, raw_results):
#             if raw_result is None:
#                 results.append({'result_id': result_id})
#                 continue
                
#             try:
#                 parsed_result = json.loads(raw_result)
#                 results.append(parsed_result)
#             except json.JSONDecodeError as e:
#                 results.append({
#                     'valid': False,
#                     'response_data': None,
#                     'failure_reason': 'Json decode error - Invalid JSON data in Redis result',
#                     'failure_detail': str(e)
#                 })
                
#         return results
        
#     finally:
#         await redis.close()
        
# async def execute_api_plugin(plugin: Plugin, execute_request: dict):
#     """Execute plugin via API call."""
#     url = plugin.endpoint_url
#     timeout = plugin.timeout
#     retries = plugin.max_retries
#     execute_request = execute_request.model_dump()
#     execute_request['request_id'] = get_request_key(plugin, execute_request.get('id', None))
#     response = await make_post_request(execute_request, url, timeout, retries)
#     return response 

# def rabbitmq_publish(messages):
#     """Publish messages to RabbitMQ."""
#     rabbitmq_params = pika.ConnectionParameters(
#         host='rabbitmq',
#         port=int(os.environ.get('RABBITMQ_PORT', 5672)),
#         credentials=pika.PlainCredentials(
#             os.environ['RABBITMQ_DEFAULT_USER'],
#             os.environ['RABBITMQ_DEFAULT_PASS']
#         )
#     )

#     try:
#         connection = pika.BlockingConnection(rabbitmq_params)
#         channel = connection.channel()
        
#         for message in messages:
#             channel.basic_publish(
#                 exchange=os.environ['RABBITMQ_EXCHANGE_NAME'],
#                 routing_key=message['request_id'],
#                 body=json.dumps(message)
#             )
#             print(f"Message published to {message['request_id']}")
#     except pika.exceptions.AMQPError as e:
#         print(f"Error publishing message: {e}")
#     finally:
#         if channel and channel.is_open:
#             channel.close()
#         if connection and connection.is_open:
#             connection.close()

# async def execute_queue_plugin(plugin: Plugin, execute_request: dict):
#     """Execute plugin via queue (RabbitMQ)."""
#     execute_request = execute_request.model_dump()
#     request_key = get_request_key(plugin, execute_request.get('id', None))
#     execute_request['request_id'] = request_key
#     rabbitmq_publish([execute_request])
#     response_key = request_key.replace('request', 'response')
#     await asyncio.sleep(0)
#     return {'result_id': response_key}

# # Plugin execution function mappings
# execute_plugin_map = {
#     'api': execute_api_plugin,
#     'queue': execute_queue_plugin,
# }

# async def batch_execute_api_plugin(plugin: Plugin, execute_request: list[dict]):
#     """Execute plugin via API with batching capabilities."""
#     batch_size = plugin.batch_size

#     post_kwargs = {
#         'url': plugin.endpoint_url,
#         'timeout': plugin.timeout,
#         'retries': plugin.max_retries
#     }

#     request_data = []
#     for item in execute_request:
#         item = item.model_dump()
#         item['request_id'] = get_request_key(plugin, item.get('id', None))
#         request_data.append(item)

#     if len(request_data) <= batch_size:
#         response = await make_post_request(request_data, **post_kwargs)
#     else:
#         batches = [request_data[i:i+batch_size] for i in range(0, len(request_data), batch_size)]
#         concurrency = min(plugin.max_concurrency, len(batches))
#         print(f"Executing {concurrency} concurrent post requests")
#         response = await concurrency_wrapper(concurrency, 
#                                              make_post_request,
#                                              batches,
#                                              post_kwargs
#                                              )
#         response = [item for sublist in response for item in sublist]

#     if not isinstance(response, list):
#         raise HTTPException(
#             status_code=502,
#             detail=f"Invalid response format from plugin. Expected list, got {type(response).__name__}"
#         )
    
#     if len(response) != len(execute_request):
#         raise HTTPException(
#             status_code=502,
#             detail=f"Plugin response length mismatch. Expected {len(execute_request)} items, got {len(response)}"
#         )
    
#     return response

# async def batch_execute_queue_plugin(plugin: Plugin, execute_request: list[dict]):
#     """Execute plugin via queue with batching capabilities."""
#     messages = []
#     response = []
#     for item in execute_request:
#         item = item.model_dump()
#         request_key = get_request_key(plugin, item.get('id', None))
#         item['request_id'] = request_key
#         messages.append(item)
#         response_key = request_key.replace('request', 'response')
#         response.append({'result_id': response_key})
#     rabbitmq_publish(messages)
#     await asyncio.sleep(0)
#     return response 

# # Batch execution function mappings
# batch_execute_plugin_map = {
#     'api': batch_execute_api_plugin,
#     'queue': batch_execute_queue_plugin,
# }

