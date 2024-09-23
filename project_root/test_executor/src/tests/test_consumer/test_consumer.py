import time 
from tests.utils import (
                            fetch_test_consumer_plugins, 
                            type_to_request_func, 
                            publish_and_poll
                        )
from tests.schemas import schema_mapping

def test_consumer_plugins_created(backend_client):
    plugins = fetch_test_consumer_plugins(backend_client)
    assert len(plugins) > 0, "No test consumer plugins found"
    type_counts = {}
    for plugin in plugins:
        assert plugin['name'].startswith('mock_') and '_queue_' in plugin['name'], f"Unexpected plugin name: {plugin['name']}"

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


def test_backend_execution(backend_client):
    plugin = fetch_test_consumer_plugins(backend_client)[0]
    request_data = type_to_request_func[plugin['type']](plugin)

    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200
    result_id = response.json()['result_id']

    for i in range(20):
        result = backend_client.get(f"/api/v1/execute/{result_id}")
        assert response.status_code == 200
        result = result.json()
        if 'result_id' not in result:
            return 
        time.sleep(0.1)
    assert 'result_id' not in result 
    assert result['valid']


