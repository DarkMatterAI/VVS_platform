from tests.utils import (
                            fetch_test_consumer_plugins, 
                            type_to_request_func, 
                            publish_and_poll
                        )

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


