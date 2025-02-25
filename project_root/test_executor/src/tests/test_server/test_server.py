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
        assert plugin['name'].startswith('mock_'), f"Unexpected plugin name: {plugin['name']}"
        assert '_api_' in plugin['name'], f"Unexpected plugin name: {plugin['name']}"

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

def helper_test_api_route(backend_client, test_api_client, plugin_type, 
                          route, batched=False, batch_size=1, status_code=200):
    schemas = schema_mapping[plugin_type]
    plugin = fetch_test_api_plugins(backend_client, plugin_type=plugin_type)[0]
    request_data = type_to_request_func[plugin_type](plugin)
    schemas['request'].model_validate(request_data)

    if batched:
        request_data = [request_data for i in range(batch_size)]

    response = test_api_client.post(route, json=request_data)
    assert response.status_code == status_code

    if status_code == 200:
        if batched:
            [schemas['response'].model_validate(i) for i in response.json()]
        else:
            schemas['response'].model_validate(response.json())

def test_embedding_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'embedding', '/embedding')

def test_data_source_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'data_source', '/data_source')

def test_filter_api(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'filter', '/filter')

def test_filter_api_batched(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'filter', '/filter', batched=True)
    
def test_filter_api_large_batch_fail(backend_client, test_api_client):
    helper_test_api_route(backend_client, test_api_client, 'filter', '/filter', 
                          batched=True, batch_size=10, status_code=422)

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

def test_filter_backend_execution_batched(backend_client):
    plugin = fetch_test_api_plugins(backend_client, plugin_type='filter')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}/batch", json=[request_data])
    assert response.status_code == 200

def test_filter_backend_execution_batch_split(backend_client, test_api_client):
    # direct request over batch size fails
    helper_test_api_route(backend_client, test_api_client, 'filter', '/filter', 
                          batched=True, batch_size=10, status_code=422)

    # test backend correctly breaks up request
    plugin = fetch_test_api_plugins(backend_client, plugin_type='filter')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}/batch", 
                                   json=[request_data for i in range(10)])
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
    execute_data = {
        "request_id":"request.api.embedding.1.1.1",
        "id":1,
        "external_id":"1",
        "item":"item"
    }
    response = backend_client.post(f"/api/v1/execute/1234567890", json=execute_data)
    assert response.status_code == 404
