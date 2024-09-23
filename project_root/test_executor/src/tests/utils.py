import os 
import uuid 
import json 
import numpy as np 
import string 
import time 
from typing import List, Dict, Any 

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))

def fetch_plugins_by_filter(backend_client, name_pattern: str=None, group_key: str=None, plugin_type: str=None):
    params = {
        'name' : name_pattern,
        'group_key' : group_key,
        'plugin_type' : plugin_type
    }
    params = {k:v for k,v in params.items() if v is not None}
    response = backend_client.get("/api/v1/plugins/", params=params)
    response.raise_for_status()
    return response.json()

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

# request.<group_key>.<plugin_type>.<plugin_id>.<item_id>.<request_id>

def get_request_id(plugin_record):
    if plugin_record['execution_type'].lower() == 'queue':
        k1 = plugin_record['group_key']
    else:
        k1='api'

    k2 = plugin_record['type']
    k3 = plugin_record['id']
    k4 = np.random.randint(1e5) # item id
    k5 = uuid.uuid4() # request id

    request_id = f"request.{k1}.{k2}.{k3}.{k4}.{k5}"
    return request_id 

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

def get_random_item(with_embedding=False, with_named_embedding=False):
    random_item = {
        'id' : str(uuid.uuid4()),
        'external_id' : str(uuid.uuid4()),
        'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
    }
    if with_embedding:
        random_item['embedding'] = np.random.randn(EMBEDDING_SIZE).tolist()

    if with_named_embedding:
        random_item['embedding'] = [{
            'id' : 0,
            'name' : '',
            'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()
        }]

    return random_item 

def get_random_parents():
    parents = [get_random_item() for i in range(NUM_PARENTS)]
    for i, parent in enumerate(parents):
        parent['assembly_index'] = i
    return parents 

def get_embedding_request(plugin_record):
    request_data = get_random_item()
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def get_data_source_request(plugin_record, k=5):
    request_data = {
        'request_id' : get_request_id(plugin_record),
        'embedding' : [{
            'id' : plugin_record['embedding_ids'][0],
            'name' : '',
            'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()
        }],
        'k' : k
    }
    return request_data 

def get_filter_request(plugin_record):
    request_data = get_random_item(with_named_embedding=True)
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def get_score_request(plugin_record):
    request_data = get_random_item(with_named_embedding=True)
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def get_mapper_request(plugin_record):
    request_data = {
        'request_id' : get_request_id(plugin_record),
        'embedding' : {
            'id' : plugin_record['input_embedding_id'],
            'name' : '',
            'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()
        },
    }
    return request_data 

def get_assembly_request(plugin_record):
    request_data = {
        'request_id' : get_request_id(plugin_record),
        'parents' : get_random_parents()
    }
    return request_data 

type_to_request_func = {
    'embedding' : get_embedding_request,
    'data_source' : get_data_source_request,
    'filter' : get_filter_request,
    'score' : get_score_request,
    'mapper' : get_mapper_request,
    'assembly' : get_assembly_request
}
