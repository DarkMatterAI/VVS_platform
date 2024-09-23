import itertools 
import pytest 
import uuid 
import time 

from tests.utils import publish_and_poll, fetch_plugins_by_filter, get_request_id

api_str = '/api/v1/rdkit_plugins'

@pytest.fixture(scope="function")
def rdkit_test_filter(backend_client):
    def _create_filter():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test RDKit Filter Integration {next(itertools.count(1))}",
                "type": "filter",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "config": {"property_filters": [{"property_name": "TPSA", "min_val": 10}]}
            }
        )
        assert response.status_code == 200
        return response.json()

    return _create_filter

def get_request_data(plugin_record):
    request_data = {
        'id' : str(uuid.uuid4()),
        'external_id' : str(uuid.uuid4()),
        'item' : 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1',
        'embedding' : []
    }
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def delete_plugin(plugin_record, backend_client):
    response = backend_client.delete(f"{api_str}/{plugin_record['id']}")
    assert response.status_code == 200 

def test_rdkit_filter_consumer(redis_connection, rabbitmq_connection, rdkit_test_filter, backend_client):
    filter_record = rdkit_test_filter()
    request_data = get_request_data(filter_record)

    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == True
    delete_plugin(filter_record, backend_client)

def test_rdkit_filter_backend_execution(backend_client, rdkit_test_filter):
    filter_record = rdkit_test_filter()
    request_data = get_request_data(filter_record)

    response = backend_client.post(f"/api/v1/execute/{filter_record['id']}", json=request_data)
    assert response.status_code == 200
    result_id = response.json()['result_id']

    for i in range(20):
        result = backend_client.get(f"/api/v1/execute/{result_id}")
        assert response.status_code == 200
        result = result.json()
        if 'result_id' not in result:
            break 
        time.sleep(0.1)
    assert 'result_id' not in result 
    assert result['valid']
    delete_plugin(filter_record, backend_client)



