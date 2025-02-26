import os 
import uuid 
import json 
import numpy as np 
import string 
import time 
from typing import List, Dict, Any

from vvs_database.schemas import PluginType

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))

def fetch_plugins_by_filter(backend_client, 
                            name_pattern: str=None, 
                            group_key: str=None, 
                            plugin_type: str=None,
                            plugin_class: str=None
                            ):
    params = {
        'name' : name_pattern,
        'group_key' : group_key,
        'plugin_type' : plugin_type,
        'plugin_class' : plugin_class 
    }
    params = {k:v for k,v in params.items() if v is not None}
    response = backend_client.get("/api/v1/plugins/", params=params)
    response.raise_for_status()
    return response.json()

def delete_plugin(plugin_record, backend_client, api_str):
    response = backend_client.delete(f"{api_str}/{plugin_record['id']}")
    assert response.status_code == 200 

def fetch_test_api_plugins(backend_client, plugin_type: str = None) -> List[Dict[Any, Any]]:
    return fetch_plugins_by_filter(backend_client, name_pattern=f"mock_%_api_%", plugin_type=plugin_type)

def fetch_test_consumer_plugins(backend_client, plugin_type: str = None) -> List[Dict[Any, Any]]:
    return fetch_plugins_by_filter(backend_client, name_pattern=f"mock_%_queue_%", plugin_type=plugin_type)

def rabbitmq_publish(channel, routing_key, message):
    channel.basic_publish(
        exchange=os.environ['RABBITMQ_EXCHANGE_NAME'], 
        routing_key=routing_key, 
        body=json.dumps(message)
        )
    
def poll_redis(redis_connection, response_key, interval=0.1, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = redis_connection.get(response_key)
        if response:
            redis_connection.delete(response_key)
            return json.loads(response)
        
        time.sleep(interval)

    raise TimeoutError(f"No response received for key {response_key} after {timeout} seconds")

def poll_backend(backend_client, result_id, interval=0.1, timeout=4):
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = backend_client.get(f"/api/v1/execute/{result_id}")
        assert result.status_code == 200
        result = result.json()
        if 'result_id' not in result:
            return result
        time.sleep(interval)
    return result 

def backend_execute_and_poll(backend_client, plugin, request_data, interval=0.1, timeout=4):
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200
    result_id = response.json()['result_id']
    return poll_backend(backend_client, result_id, interval=interval, timeout=timeout)

def get_request_data(plugin_record, item_id=None):
    group_key = plugin_record['group_key']
    plugin_type = plugin_record['type' ]
    plugin_id = plugin_record['id']  
    plugin_name = plugin_record['name']
    request_id = uuid.uuid4()

    if item_id is None:
        item_id = uuid.uuid4()
    request_key = f"request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}"
    request_data = {
        'request_id' : request_key,
        'plugin_id' : plugin_id,
        'plugin_name' : plugin_name
    }
    return request_data 

def request_id_to_response_key(request_id):
    return request_id.replace('request', 'response').replace('.', ':')

def publish_and_poll(redis_connection, rabbitmq_connection, 
                     request_key, request_data, 
                     interval=0.1, timeout=5):
    print(f"Publishing message with {request_key}")
    rabbitmq_publish(rabbitmq_connection, request_key, request_data)
    response_key = request_id_to_response_key(request_key)
    response_data = poll_redis(redis_connection, response_key, interval, timeout)
    return response_data 


def get_random_embedding(embedding_id=0, embedding_name=''):
    return {
        'plugin_id' : embedding_id,
        'plugin_name' : embedding_name,
        'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()
    }

def get_random_item(with_named_embedding=False):
    random_item = {
        'item_id' : np.random.randint(0, 10000),
        'external_id' : str(uuid.uuid4()),
        'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
        'embedding' : None
    }

    if with_named_embedding:
        random_item['embedding'] = [get_random_embedding()]

    return random_item

def get_random_parents():
    parents = [get_random_item() for i in range(NUM_PARENTS)]
    for i, parent in enumerate(parents):
        parent['assembly_index'] = i
    return parents 

def get_embedding_request(plugin_record):
    item_data = get_random_item()
    request_data = {
        'request_data' : get_request_data(plugin_record, item_data['item_id']),
        'item_data' : item_data
    }
    return request_data 

def get_data_source_request(plugin_record, k=5, embedding_index=0):
    request_data = {
        'request_data' : get_request_data(plugin_record),
        'embedding' : get_random_embedding(embedding_id=embedding_index),
        'k' : k
        }
    return request_data 

def get_filter_request(plugin_record):
    item_data = get_random_item(with_named_embedding=True)
    request_data = {
        'request_data' : get_request_data(plugin_record, item_data['item_id']),
        'item_data' : item_data
    }
    return request_data 

def get_score_request(plugin_record):
    item_data = get_random_item(with_named_embedding=True)
    request_data = {
        'request_data' : get_request_data(plugin_record, item_data['item_id']),
        'item_data' : item_data
    }
    return request_data 

def get_mapper_request(plugin_record):
    request_data = {
        'request_data' : get_request_data(plugin_record),
        'embedding' : get_random_embedding(embedding_id=plugin_record['input_embedding_id']),
        }
    return request_data 

def get_assembly_request(plugin_record):
    request_data = {
        'request_data' : get_request_data(plugin_record),
        'parents' : get_random_parents()
    }
    return request_data 

# Map plugin types to request generator functions
type_to_request_func = {
    PluginType.EMBEDDING: get_embedding_request,
    PluginType.DATA_SOURCE: get_data_source_request,
    PluginType.FILTER: get_filter_request,
    PluginType.SCORE: get_score_request,
    PluginType.MAPPER: get_mapper_request,
    PluginType.ASSEMBLY: get_assembly_request,
    # String versions for backward compatibility
    'embedding': get_embedding_request,
    'data_source': get_data_source_request,
    'filter': get_filter_request,
    'score': get_score_request,
    'mapper': get_mapper_request,
    'assembly': get_assembly_request
}

