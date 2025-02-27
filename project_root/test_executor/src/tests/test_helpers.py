import time
from tests.utils import type_to_request_func, poll_backend, publish_and_poll

from vvs_database.schemas import request_response_schema_mapping as schema_mapping

def plugin_creation_helper(backend_client, plugin_pattern, plugin_type_counts):
    """Test if plugins are properly created with expected counts by type."""
    plugins = backend_client.get("/api/v1/plugins/", 
                                params={'name': plugin_pattern}).json()
    
    assert len(plugins) > 0, f"No plugins matching {plugin_pattern} found"
    
    type_counts = {}
    pattern_chunks = plugin_pattern.split('%')
    for plugin in plugins:
        for p in pattern_chunks:
            assert p in plugin['name'], f"Unexpected plugin name: {plugin['name']}"
        type_counts[plugin['type']] = type_counts.get(plugin['type'], 0) + 1
    
    for k, v in plugin_type_counts.items():
        assert type_counts.get(k, 0) == v, f"Expected {v} plugins of type {k}, got {type_counts.get(k, 0)}"
    
    return plugins

def execute_plugin_helper(backend_client, plugins, plugin_type, 
                              batched=False, batch_endpoint=None, 
                              timeout=4, batch_size=1, custom_request=None,
                              checkin_result=False):
    """Test plugin execution via backend client."""
    plugin = next((p for p in plugins if p['type'] == plugin_type), None)
    assert plugin is not None, f"No plugin of type {plugin_type} found"
    
    # Use custom request if provided, otherwise generate one
    if custom_request:
        request_data = custom_request
    else:
        request_data = type_to_request_func[plugin_type](plugin)

    print(request_data)
    
    if batched:
        endpoint = f"/api/v1/execute/{plugin['id']}/batch" if not batch_endpoint else batch_endpoint
        request_payload = [request_data] * batch_size
        response = backend_client.post(endpoint, json=request_payload, 
                                       params={"checkin_result": checkin_result})
    else:
        endpoint = f"/api/v1/execute/{plugin['id']}"
        response = backend_client.post(endpoint, json=request_data, 
                                       params={"checkin_result": checkin_result})
    
    assert response.status_code == 200, response.text
    
    # For batched execution, poll for results
    if batched and timeout > 0:
        result_ids = response.json()
        start = time.time()
        
        while (time.time() - start < timeout) and any('result_id' in item for item in result_ids):
            batch_result = backend_client.post(f"/api/v1/execute/result_batch", json=result_ids)
            assert batch_result.status_code == 200
            result_ids = [i for i in batch_result.json() if 'valid' not in i]
            if not result_ids:
                break
            time.sleep(0.1)
        
        return result_ids
    
    # For non-batched execution with timeout, poll until result is ready
    if timeout > 0 and isinstance(response.json(), dict) and 'result_id' in response.json():
        result = poll_backend(backend_client, response.json()['result_id'], timeout=timeout)
        return result
    
    return response.json()

def direct_request_helper(api_client, backend_client, plugins, plugin_type, 
                             endpoint, batched=False, batch_size=1, status_code=200):
    """Test direct API requests to plugin service."""
    schemas = schema_mapping[plugin_type]
    plugin = next((p for p in plugins if p['type'] == plugin_type), None)
    assert plugin is not None, f"No plugin of type {plugin_type} found"
    
    request_data = type_to_request_func[plugin_type](plugin)
    schemas['request'].model_validate(request_data)

    if batched:
        request_data = [request_data for i in range(batch_size)]

    response = api_client.post(endpoint, json=request_data)
    assert response.status_code == status_code

    if status_code == 200:
        if batched:
            [schemas['response'].model_validate(i) for i in response.json()]
        else:
            schemas['response'].model_validate(response.json())
    
    return response.json()

def queue_request_helper(redis_connection, rabbitmq_connection, backend_client, 
                             plugins, plugin_type, interval=0.1, timeout=5):
    """Test message queue based plugin execution."""
    schemas = schema_mapping[plugin_type]
    plugin = next((p for p in plugins if p['type'] == plugin_type), None)
    assert plugin is not None, f"No plugin of type {plugin_type} found"
    
    request_data = type_to_request_func[plugin_type](plugin)
    schemas['request'].model_validate(request_data)
    
    response_data = publish_and_poll(
        redis_connection, 
        rabbitmq_connection,
        request_data['request_data']['request_id'], 
        request_data, 
        interval, 
        timeout
    )
    
    assert response_data['valid'] == True
    schemas['response'].model_validate(response_data['response_data']), response_data['response_data']
    
    return response_data