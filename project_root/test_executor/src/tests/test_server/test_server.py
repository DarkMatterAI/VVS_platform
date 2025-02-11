from tests.utils import fetch_test_api_plugins, type_to_request_func
from tests.schemas import schema_mapping

def test_server_ping(test_api_client):
    response = test_api_client.get('/')
    assert response.status_code == 200 

def test_api_plugins_created(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    assert len(plugins) > 0, "No test API plugins found"
    type_counts = {}
    for plugin in plugins:
        assert plugin['name'].startswith('mock_') and '_api_' in plugin['name'], f"Unexpected plugin name: {plugin['name']}"

        type_counts[plugin['type']] = type_counts.get(plugin['type'], 0) + 1

    target_counts = {
        'embedding' : 3,
        'data_source' : 1,
        'filter' : 1,
        'score' : 1,
        'mapper' : 1,
        'assembly' : 1
    }

    for k,v in target_counts.items():
        assert type_counts[k] == v

def helper_test_api_route(backend_client, test_api_client, plugin_type, route):
    schemas = schema_mapping[plugin_type]
    plugin = fetch_test_api_plugins(backend_client, plugin_type=plugin_type)[0]
    request_data = type_to_request_func[plugin_type](plugin)
    schemas['request'].model_validate(request_data)
    response = test_api_client.post(route, json=request_data)
    assert response.status_code == 200 
    schemas['response'].model_validate(response.json())

def test_embedding_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'embedding', '/embedding')

def test_data_source_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'data_source', '/data_source')

def test_filter_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'filter', '/filter')

def test_score_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'score', '/score')

def test_mapper_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'mapper', '/mapper')

def test_assembly_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'assembly', '/assembly')

def test_embedding_backend_execution(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='embedding')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_data_source_backend_execution(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='data_source')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_filter_backend_execution(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='filter')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_score_backend_execution(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='score')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_mapper_backend_execution(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='mapper')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_assembly_backend_execution(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='assembly')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200

def test_backend_execution_invalid_plugin_id(backend_client):
    response = backend_client.post(f"/api/v1/execute/1234567890", json={"id":1,"external_id":"1","item":"item"})
    assert response.status_code == 404

