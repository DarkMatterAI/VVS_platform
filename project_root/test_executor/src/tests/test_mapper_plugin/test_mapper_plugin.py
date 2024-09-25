import numpy as np 
from tests.utils import fetch_plugins_by_filter


def test_mapper_plugins_created(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, plugin_class='internal_mapper')
    assert len(plugins) > 0, "No internal mapper plugin found"

def test_backend_execution(backend_client):
    plugin = fetch_plugins_by_filter(backend_client, plugin_class='internal_mapper')[0]

    source_embedding_id = plugin['input_embedding_id']
    embedding_response = backend_client.get(f"/api/v1/plugins/{source_embedding_id}")
    assert embedding_response.status_code == 200
    embedding_record = embedding_response.json()

    vector_length = embedding_record['vector_length']
    request_data = {
        'request_id' : '',
        'id' : source_embedding_id,
        'name' : embedding_record['name'],
        'embedding' : np.random.rand(vector_length).tolist()
    }
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200
