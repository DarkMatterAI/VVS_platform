import os 
import string 
import numpy as np 
import asyncio 
import itertools 
import random 
import uuid 

from tests_new.utils.backend_utils import backend_get_plugins_by_filter

from vvs_database import crud, schemas
from vvs_database.utils import plugin_type_map

_item_counter = itertools.count(1)
random_item_key = ''.join(np.random.choice([i for i in string.ascii_lowercase], 8))

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))

def get_test_embedding(embedding_id, embedding_size=None):
    if embedding_size is None:
        embedding_size = EMBEDDING_SIZE
    return {'plugin_id' : embedding_id,
            'plugin_name' : '',
            'embedding' : [random.random() for i in range(embedding_size)]}

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

async def generate_item(db_session):
    item_name = f"Test Item {next(_item_counter)} {random_item_key}"
    item = await crud.create_item(db_session, item_name)
    return item 

async def generate_data_source_request(db_session, plugin, n_requests):
    embedding_plugin = await crud.get_plugin(db_session, plugin['embedding_ids'][0])
    embedding_size = embedding_plugin.vector_length
    requests = []
    for i in range(n_requests):
        request = {
            'request_data' : get_request_data(plugin),
            'embedding' : get_test_embedding(plugin['embedding_ids'][0], embedding_size),
            'k' : 5
        }
        requests.append(request)
    await asyncio.sleep(0)
    return requests 

async def generate_assembly_request(db_session, plugin, n_requests):
    requests = []
    n_parents = plugin['num_parents']
    for i in range(n_requests):
        request = {'request_data' : get_request_data(plugin),
                   'parents' : []}
        for parent_idx in range(n_parents):
            parent_item = await generate_item(db_session)
            request['parents'].append({
                'item_id' : parent_item.id,
                'external_id' : None,
                'item' : parent_item.item,
                'assembly_index' : parent_idx
            })
        requests.append(request)
    return requests

async def generate_mapper_request(db_session, plugin, n_requests):
    embedding_plugin = await crud.get_plugin(db_session, plugin['input_embedding_id'])
    embedding_size = embedding_plugin.vector_length
    requests = []
    for i in range(n_requests):
        request = {
            'request_data' : get_request_data(plugin),
            'embedding' : get_test_embedding(plugin['input_embedding_id'], embedding_size)
        }
        requests.append(request)
    await asyncio.sleep(0)
    return requests 

async def generate_item_request(db_session, plugin, n_requests):
    requests = []
    for i in range(n_requests):
        item = await generate_item(db_session)
        request = {
            'request_data' : get_request_data(plugin, item.id),
            'item_data' : {
                'item_id' : item.id,
                'external_id' : None,
                'item' : item.item,
                'embedding' : None
            }
        }
        linked_embeddings = plugin.get('embedding_ids', [])
        if linked_embeddings:
            embedding_plugin = await crud.get_plugin(db_session, linked_embeddings[0])
            embedding_size = embedding_plugin.vector_length
            request['item_data']['embedding'] = get_test_embedding(linked_embeddings[0],
                                                                   embedding_size)
        requests.append(request)
    
    return requests 

async def generate_request_data(db_session, plugin, n_requests, to_model=False):
    plugin_type = plugin['type']
    if plugin_type == 'data_source':
        requests = await generate_data_source_request(db_session, plugin, n_requests)
    elif plugin_type == 'assembly':
        requests = await generate_assembly_request(db_session, plugin, n_requests)
    elif plugin_type == 'mapper':
        requests = await generate_mapper_request(db_session, plugin, n_requests)
    else:
        requests = await generate_item_request(db_session, plugin, n_requests)

    if to_model:
        request_model = plugin_type_map[plugin_type]['execute_request_model']
        requests = [request_model.model_validate(i) for i in requests]

    return requests

def validate_response(plugin, response):
    if not isinstance(response, list):
        response = [response]

    response_model = plugin_type_map[plugin['type']]['execute_response_model']
    response = [response_model.model_validate(i) for i in response]
    return response 

def validate_api_response(plugin, response, status_code):
    assert response.status_code == status_code, response.text

    if status_code == 200:
        validate_response(plugin, response.json())

async def get_plugin_and_request(db_session, 
                                 backend_client, 
                                 plugin_type, 
                                 name_pattern=None,
                                 batch_size=1, 
                                 to_model=False,
                                 group_key=None,
                                 ):
    plugins = backend_get_plugins_by_filter(backend_client, name_pattern=name_pattern, group_key=group_key)
    assert len(plugins) > 0
    plugin = plugins[0]
    assert plugin['type'] == plugin_type

    request_data = await generate_request_data(db_session, plugin, batch_size, to_model=to_model)

    return plugin, request_data 

async def checkin_items(db_session, new_items, plugin_id):
    if type(new_items) != list:
        new_items = [new_items]

    if type(new_items[0]) == dict:
        new_items = [schemas.NewItem(**i) for i in new_items]

    response = await crud.item_checkin(db_session, new_items, plugin_id)
    return response 

async def generate_rdkit_item_request(db_session, smiles, plugin, to_model=False):
    new_items = [{'item' : i, 'external_id' : None} for i in smiles]
    checkin_result = await checkin_items(db_session, new_items, None)
    checkin_result = checkin_result['items']
    record_dict = {i.item:i for i in checkin_result}

    requests = [] 
    for smile in smiles:
        item = record_dict[smile]
        request = {
            'request_data' : get_request_data(plugin, item.id),
            'item_data' : {
                'item_id' : item.id,
                'external_id' : None,
                'item' : item.item,
                'embedding' : None
            }
        }
        requests.append(request)

    if to_model:
        request_model = plugin_type_map[plugin['type']]['execute_request_model']
        requests = [request_model.model_validate(i) for i in requests]

    await db_session.commit()

    return requests 
    
async def generate_rdkit_assembly_request(db_session, parents, plugin, to_model=False):
    unique_smiles = set()
    for parent_list in parents:
        unique_smiles.update(parent_list)

    new_items = [{'item' : i, 'external_id' : None} for i in list(unique_smiles)]
    checkin_result = await checkin_items(db_session, new_items, None)
    checkin_result = checkin_result['items']
    record_dict = {i.item:i for i in checkin_result}

    requests = []
    for parent_list in parents:
        request = {
            'request_data' : get_request_data(plugin),
            'parents' : [
                {'assembly_index' : i, 'item_id': record_dict[parent_list[i]].id,
                 'external_id' : None, 'item' : parent_list[i]}
                 for i in range(len(parent_list))
            ]
        }
        requests.append(request)

    if to_model:
        request_model = plugin_type_map[plugin['type']]['execute_request_model']
        requests = [request_model.model_validate(i) for i in requests]

    await db_session.commit()

    return requests 

