from tests.utils import fetch_test_api_plugins
from tests.test_helpers import (
    plugin_creation_helper,
    execute_plugin_helper,
    direct_request_helper
)

def test_server_ping(test_api_client):
    response = test_api_client.get('/')
    assert response.status_code == 200 

def test_api_plugins_created(backend_client):
    target_counts = {
        'embedding': 3,
        'data_source': 1,
        'filter': 1,
        'score': 1,
        'mapper': 1,
        'assembly': 1
    }

    plugins = plugin_creation_helper(
        backend_client, 
        "mock_%_api_%",
        target_counts
    )

# Direct API tests with common helper
def test_embedding_api(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'embedding', '/embedding')

def test_data_source_api(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'data_source', '/data_source')

def test_filter_api(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'filter', '/filter')

def test_filter_api_batched(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'filter', '/filter', batched=True)
    
def test_filter_api_large_batch_fail(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(
        test_api_client, 
        backend_client, 
        plugins, 
        'filter', 
        '/filter', 
        batched=True, 
        batch_size=10, 
        status_code=422
    )

def test_score_api(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'score', '/score')

def test_mapper_api(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'mapper', '/mapper')

def test_assembly_api(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    direct_request_helper(test_api_client, backend_client, plugins, 'assembly', '/assembly')

# Backend execution tests
def test_embedding_backend_execution(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'embedding')
    assert result is not None

def test_data_source_backend_execution(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'data_source')
    assert result is not None

def test_filter_backend_execution(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'filter')
    assert result is not None

def test_filter_backend_execution_batched(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'filter', batched=True)
    assert result is not None

def test_filter_backend_execution_batch_split(backend_client, test_api_client):
    plugins = fetch_test_api_plugins(backend_client)
    
    # Direct request over batch size fails
    direct_request_helper(
        test_api_client, 
        backend_client, 
        plugins, 
        'filter', 
        '/filter', 
        batched=True, 
        batch_size=10, 
        status_code=422
    )

    result = execute_plugin_helper(
        backend_client, 
        plugins, 
        'filter', 
        batched=True, 
        batch_size=10
    )
    assert result is not None

def test_score_backend_execution(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'score')
    assert result is not None

def test_mapper_backend_execution(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'mapper')
    assert result is not None

def test_assembly_backend_execution(backend_client):
    plugins = fetch_test_api_plugins(backend_client)
    result = execute_plugin_helper(backend_client, plugins, 'assembly')
    assert result is not None

def test_backend_execution_invalid_plugin_id(backend_client):
    # execute_data = {
    #     "request_id":"request.api.embedding.1.1.1",
    #     "id":1,
    #     "external_id":"1",
    #     "item":"item"
    # }
    execute_data = {
        'request_data' : {
            'request_id' : "request.api.embedding.1.1.1",
            'plugin_id' : 1234567890,
            'plugin_name' : ''
        },
        'item_data' : {
            'item_id' : 1,
            'external_id' : '1',
            'item' : 'item',
            'embedding' : None 
        }
    }
    response = backend_client.post(f"/api/v1/execute/1234567890", json=execute_data)
    assert response.status_code == 404

