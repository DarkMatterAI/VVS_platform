import time 
from tests.utils import (
                            fetch_test_consumer_plugins, 
                            type_to_request_func, 
                            publish_and_poll,
                            poll_backend,
                            backend_execute_and_poll
                        )
from tests.schemas import schema_mapping

def test_consumer_plugins_created(backend_client):
    plugins = fetch_test_consumer_plugins(backend_client)
    assert len(plugins) > 0, "No test consumer plugins found"
    type_counts = {}
    for plugin in plugins:
        assert plugin['name'].startswith('mock_') and '_queue_' in plugin['name'], f"Unexpected plugin: {plugin['name']}"

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

def test_response_consumer(redis_connection, rabbitmq_connection, backend_client):

    plugin = fetch_test_consumer_plugins(backend_client)[0]
    request_data = type_to_request_func[plugin['type']](plugin)

    # send directory to response consumer 
    request_data['request_id'] = request_data['request_id'].replace('request', 'response')

    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == True

def test_request_response_loop(redis_connection, rabbitmq_connection, backend_client):

    plugin = fetch_test_consumer_plugins(backend_client)[0]
    request_data = type_to_request_func[plugin['type']](plugin)

    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == True

def test_alt_queue(redis_connection, rabbitmq_connection, backend_client):

    plugin = fetch_test_consumer_plugins(backend_client)[0]
    request_data = type_to_request_func[plugin['type']](plugin)

    # send do alt exchange
    request_data['request_id'] = request_data['request_id'].replace('request', 'blah')

    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == False 
    assert response_data['failure_reason'] == 'Alt Ex'

def test_dlx_queue(redis_connection, rabbitmq_connection, backend_client):

    plugin = fetch_test_consumer_plugins(backend_client)[0]
    request_data = type_to_request_func[plugin['type']](plugin)

    # send to dead letter 
    request_data['request_id'] = request_data['request_id'].replace(plugin['type'], 'dlx_test')

    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == False 
    assert response_data['failure_reason'] == 'Dead Letter'

def helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, plugin_type):
    schemas = schema_mapping[plugin_type]
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type=plugin_type)[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    schemas['request'].model_validate(request_data)
    response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
    assert response_data['valid'] == True
    schemas['response'].model_validate(response_data['response_data'])

def test_embedding_consumer(redis_connection, rabbitmq_connection, backend_client):
    helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, 'embedding')

def test_data_source_consumer(redis_connection, rabbitmq_connection, backend_client):
    helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, 'data_source')

def test_filter_consumer(redis_connection, rabbitmq_connection, backend_client):
    helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, 'filter')

def test_score_consumer(redis_connection, rabbitmq_connection, backend_client):
    helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, 'score')

def test_mapper_consumer(redis_connection, rabbitmq_connection, backend_client):
    helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, 'mapper')

def test_assembly_consumer(redis_connection, rabbitmq_connection, backend_client):
    helper_test_consumer_plugins(redis_connection, rabbitmq_connection, backend_client, 'assembly')

def test_embedding_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='embedding')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    result = backend_execute_and_poll(backend_client, plugin, request_data, timeout=4)
    assert 'result_id' not in result 
    assert result['valid']

def test_data_source_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='data_source')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    result = backend_execute_and_poll(backend_client, plugin, request_data, timeout=4)
    assert 'result_id' not in result 
    assert result['valid']

def test_filter_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='filter')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    result = backend_execute_and_poll(backend_client, plugin, request_data, timeout=4)
    assert 'result_id' not in result 
    assert result['valid']

def test_filter_backend_execution_batched(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='filter')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}/batch", 
                                   json=[request_data for i in range(10)])
    assert response.status_code == 200
    result_ids = response.json()

    interval = 0.1
    timeout = 8
    start = time.time()
    while (time.time() - start < timeout) and (len(result_ids)>0):
        result = backend_client.post(f"/api/v1/execute/result_batch",
                                     json=result_ids)
        assert result.status_code == 200
        result_ids = [i for i in result.json() if 'valid' not in i]
        if len(result_ids) == 0:
            break 
        time.sleep(interval)

def test_score_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='score')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    result = backend_execute_and_poll(backend_client, plugin, request_data, timeout=4)
    assert 'result_id' not in result 
    assert result['valid']

def test_mapper_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='mapper')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    result = backend_execute_and_poll(backend_client, plugin, request_data, timeout=4)
    assert 'result_id' not in result 
    assert result['valid']  

def test_assembly_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client, plugin_type='assembly')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    result = backend_execute_and_poll(backend_client, plugin, request_data, timeout=4)
    assert 'result_id' not in result 
    assert result['valid']

