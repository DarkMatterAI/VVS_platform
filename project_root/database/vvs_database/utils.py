from sqlalchemy.orm import class_mapper
import httpx 
import asyncio

from vvs_database import schemas, models, logging

plugin_type_map = {
    schemas.PluginType.EMBEDDING : {
        'create_model' : schemas.EmbeddingPluginCreate,
        'response_model' : schemas.EmbeddingPluginInDB,
        'data_model' : models.EmbeddingPlugin,
        'execute_request_model' : schemas.ItemRequest,
        'execute_response_model' : schemas.EmbedResponse,
    },
    schemas.PluginType.DATA_SOURCE : {
        'create_model' : schemas.DataSourcePluginCreate,
        'response_model' : schemas.DataSourcePluginInDB,
        'data_model' : models.DataSourcePlugin,
        'execute_request_model' : schemas.DataSourceRequest,
        'execute_response_model' : schemas.DataSourceResponse,
    },
    schemas.PluginType.FILTER : {
        'create_model' : schemas.FilterPluginCreate,
        'response_model' : schemas.FilterPluginInDB,
        'data_model' : models.FilterPlugin,
        'execute_request_model' : schemas.ItemRequest,
        'execute_response_model' : schemas.FilterResponse,
    },
    schemas.PluginType.SCORE : {
        'create_model' : schemas.ScorePluginCreate,
        'response_model' : schemas.ScorePluginInDB,
        'data_model' : models.ScorePlugin,
        'execute_request_model' : schemas.ItemRequest,
        'execute_response_model' : schemas.ScoreResponse,
    },
    schemas.PluginType.MAPPER : {
        'create_model' : schemas.MapperPluginCreate,
        'response_model' : schemas.MapperPluginInDB,
        'data_model' : models.MapperPlugin,
        'execute_request_model' : schemas.MapperRequest,
        'execute_response_model' : schemas.MapperResponse,
    },
    schemas.PluginType.ASSEMBLY : {
        'create_model' : schemas.AssemblyPluginCreate,
        'response_model' : schemas.AssemblyPluginInDB,
        'data_model' : models.AssemblyPlugin,
        'execute_request_model' : schemas.AssemblyRequest,
        'execute_response_model' : schemas.AssemblyResponse,
    }
}

job_type_map = {
    schemas.JobType.TEST_JOB : {
        'data_model' : models.TestJob
    },
    schemas.JobType.QDRANT_UPLOAD : {
        'data_model' : models.QdrantUploadJob
    },
    schemas.JobType.HILL_CLIMB_JOB : {
        'data_model' : models.HCJob
    },
    schemas.JobType.HILL_CLIMB_JOB_INPUT : {
        'data_model' : models.HCInputJob
    },
    schemas.JobType.HILL_CLIMB_JOB_ITERATION : {
        'data_model' : models.HCIterationJob
    }
}


def object_as_dict(obj):
    """Convert SQLAlchemy model instance to dictionary."""
    output = {}
    for c in class_mapper(obj.__class__).columns:
        output[c.key] = getattr(obj, c.key)
    return output 

def get_plugin_data_model(plugin_type: schemas.PluginType):
    """Get the SQLAlchemy model for a specific plugin type."""
    return plugin_type_map[plugin_type]['data_model']

def remap_embeddings(plugin: models.Plugin, plugin_dict: dict) -> None:
    """Add embedding relationship data to plugin dictionary."""
    if isinstance(plugin, models.DataSourcePlugin):
        plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings]
    elif isinstance(plugin, (models.FilterPlugin, models.ScorePlugin)):
        plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings] if plugin.embeddings else None
    elif isinstance(plugin, models.MapperPlugin):
        plugin_dict['input_embedding_id'] = plugin.input_embedding_id
        plugin_dict['embedding_ids'] = [e.id for e in plugin.embeddings]

def get_plugin_response_model(plugin: models.Plugin):
    """Convert SQLAlchemy plugin instance to Pydantic response model."""
    plugin_dict = object_as_dict(plugin)
    remap_embeddings(plugin, plugin_dict)
    return plugin_type_map[plugin.type]['response_model'].model_validate(plugin_dict)

def validate_updates(plugin: models.Plugin, update_data: dict):
    """Validate that plugin updates are consistent with the model schema."""
    plugin_dict = object_as_dict(plugin)
    remap_embeddings(plugin, plugin_dict)
    plugin_dict.update(update_data)
    plugin_type_map[plugin.type]['response_model'].model_validate(plugin_dict)

async def make_post_request(data: dict, url: str, timeout: int, retries: int, retry_sleep=0, log_id=None):
    """Make HTTP POST request with retry logic."""
    if log_id is None:
        log_id = ''
    else:
        log_id = f"{log_id}: "
    async with httpx.AsyncClient() as client:
        for attempt in range(retries + 1):
            logging.info(f"{log_id}Post Request to {url} attempt {attempt+1}")
            try:
                response = await client.post(url, json=data, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                # Handle HTTP status errors (400-599)
                status_code = e.response.status_code
                error_message = f"HTTP {status_code}"
                
                if status_code == 400:
                    error_message += " Bad Request: The server rejected the request as malformed"
                elif status_code == 401:
                    error_message += " Unauthorized: Authentication is required"
                elif status_code == 403:
                    error_message += " Forbidden: You don't have permission to access this resource"
                elif status_code == 404:
                    error_message += " Not Found: The requested resource does not exist"
                elif status_code == 429:
                    error_message += " Too Many Requests: Rate limit exceeded"
                elif 500 <= status_code < 600:
                    error_message += " Server Error: The server failed to fulfill the request"
                
                error_message += f"\nURL: {url}\nResponse: {e.response.text}"
                
                if attempt == retries:
                    raise httpx.HTTPStatusError(error_message, request=e.request, response=e.response)
            except httpx.TimeoutException as e:
                if attempt == retries:
                    raise httpx.TimeoutException(f"Request to {url} timed out after {timeout} seconds")
            except httpx.ConnectError as e:
                if attempt == retries:
                    raise httpx.ConnectError(f"Failed to connect to {url}: {str(e)}")
            except httpx.RequestError as e:
                if attempt == retries:
                    raise httpx.RequestError(f"Request to {url} failed: {str(e)}")
                
            if retry_sleep > 0:
                logging.info(f"{log_id}Post request failed, sleeping for {retry_sleep} seconds")
                await asyncio.sleep(retry_sleep) 

async def delete_redis_keys_batch(keys, redis_client):
    if keys:
        if hasattr(redis_client, 'unlink'):
            deleted = await redis_client.unlink(*keys)
        else:
            deleted = await redis_client.delete(*keys)
    
async def clear_plugin_cache(plugin_id, redis_client, batch_size=500):
    pattern = f"plugin:{plugin_id}:*"

    total_deleted = 0
    keys_batch = []

    async for key in redis_client.scan_iter(match=pattern):
        keys_batch.append(key)

        if len(keys_batch) >= batch_size:
            await delete_redis_keys_batch(keys_batch, redis_client)
            total_deleted += len(keys_batch)
            keys_batch = []
            
    await delete_redis_keys_batch(keys_batch, redis_client)
    total_deleted += len(keys_batch)

    return total_deleted

