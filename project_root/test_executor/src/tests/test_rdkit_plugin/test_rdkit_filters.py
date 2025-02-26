import itertools 
import pytest 
import uuid 
import numpy as np 

from tests.utils import publish_and_poll, get_request_data, delete_plugin
from tests.test_helpers import execute_plugin_helper
from vvs_database.schemas import PluginType

api_str = '/api/v1/rdkit_plugins'
plugin_api_str = '/api/v1/plugins'

@pytest.fixture(scope="function")
def rdkit_test_filter(backend_client):
    def _create_filter():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test RDKit Filter Integration {next(itertools.count(1))}",
                "plugin_class": "internal_rdkit",
                "type": "filter",
                "execution_type": "queue",
                "group_key": "rdkit_plugin_filter",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "config": {"property_filters": [{"property_name": "TPSA", "min_val": 10}]}
            }
        )
        assert response.status_code == 200
        return response.json()

    return _create_filter

# def get_request_data(plugin_record):
#     request_data = {
#         'id': str(uuid.uuid4()),
#         'external_id': str(uuid.uuid4()),
#         'item': 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1',
#         'embedding': []
#     }
#     request_data['request_id'] = get_request_id(plugin_record)
#     return request_data 

def get_filter_request_data(plugin_record):
    request_data = {
        'request_data' : {},
        'item_data' : {
            'item_id' : np.random.randint(0, 10000),
            'external_id': str(uuid.uuid4()),
            'item' : 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1',
            'embedding' : []
        }
    }
    request_data['request_data'] = get_request_data(plugin_record, 
                                                    item_id=request_data['item_data']['item_id'])
    return request_data 

def test_rdkit_filter_consumer(redis_connection, rabbitmq_connection, rdkit_test_filter, backend_client):
    filter_record = rdkit_test_filter()
    request_data = get_filter_request_data(filter_record)

    response_data = publish_and_poll(
        redis_connection, 
        rabbitmq_connection, 
        request_data['request_data']['request_id'], 
        request_data
    )
    assert response_data['valid'] == True
    delete_plugin(filter_record, backend_client, plugin_api_str)

def test_rdkit_filter_backend_execution(backend_client, rdkit_test_filter):
    filter_record = rdkit_test_filter()
    
    request_data = get_filter_request_data(filter_record)
    
    plugin_result = execute_plugin_helper(
        backend_client, 
        [filter_record], 
        PluginType.FILTER, 
        timeout=20, 
        custom_request=request_data  # Pass the custom request data
    )
    
    assert 'result_id' not in plugin_result
    assert plugin_result['valid']
    assert plugin_result['response_data']['valid']
    
    delete_plugin(filter_record, backend_client, plugin_api_str)
