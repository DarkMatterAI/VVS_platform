import itertools 
import pytest 
import uuid 
import time 

from tests.utils import publish_and_poll, get_request_id, delete_plugin

api_str = '/api/v1/rdkit_plugins'

@pytest.fixture(scope="function")
def rdkit_test_assembly(backend_client):
    def _create_assembly():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test RDKit Assembly Integration {next(itertools.count(1))}",
                "type": "assembly",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "num_parents": 2,
                "config": {
                    'single_stage_reactions' : [{'smarts' : '[C:1].[N:2]>>[C:1]-[N:2]', 'requires_hs' : False}],
                    'multi_stage_reactions' : []
                }
            }
        )
        assert response.status_code == 200
        return response.json()

    return _create_assembly

def get_request_data(plugin_record):
    request_data = {
        'request_id' : None,
        'parents' : [
            {'assembly_index' : 0, 'id' : 1, 'external_id' : '1', 'item' : 'C'},
            {'assembly_index' : 1, 'id' : 2, 'external_id' : '2', 'item' : 'N'}
        ]
    }
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def test_rdkit_smarts_assembly_consumer(redis_connection, rabbitmq_connection, rdkit_test_assembly, backend_client):
    assembly_record = rdkit_test_assembly()
    request_data = get_request_data(assembly_record)

    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == True
    delete_plugin(assembly_record, backend_client, api_str)

def test_rdkit_smarts_assembly_backend_execution(backend_client, rdkit_test_assembly):
    assembly_record = rdkit_test_assembly()
    request_data = get_request_data(assembly_record)

    response = backend_client.post(f"/api/v1/execute/{assembly_record['id']}", json=request_data)
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
    delete_plugin(assembly_record, backend_client, api_str)



