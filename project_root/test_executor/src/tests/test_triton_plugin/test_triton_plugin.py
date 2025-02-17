from tests.utils import fetch_plugins_by_filter, type_to_request_func
import numpy as np 

def test_triton_ping(triton_client):
    response = triton_client.get('/v2/health/live')
    assert response.status_code == 200

def test_triton_embedding_plugins_created(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, group_key='triton_plugin_embedding')
    assert len(plugins) == 6, "Incorrect number of triton embedding plugins"

def test_triton_mapper_plugins_created(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, group_key='triton_plugin_mapper')
    assert len(plugins) == 1, "Incorrect number of triton embedding plugins"

def test_backend_embedding_execution(backend_client):
    plugin = fetch_plugins_by_filter(backend_client, group_key='triton_plugin_embedding')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_backend_mapper_execution(backend_client):
    plugin = fetch_plugins_by_filter(backend_client, group_key='triton_plugin_mapper')[0]

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
