# from tests.utils import fetch_test_consumer_plugins, type_to_request_func, publish_and_poll
# from tests.test_helpers import (
#     plugin_creation_helper,
#     execute_plugin_helper,
#     queue_request_helper
# )

# def test_response_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     plugin = plugins[0]
#     request_data = type_to_request_func[plugin['type']](plugin)

#     # send directly to response consumer 
#     request_id = request_data['request_data']['request_id']
#     response_id = request_id.replace('request', 'response')
#     request_data['request_data']['request_id'] = response_id

#     response_data = publish_and_poll(
#         redis_connection, 
#         rabbitmq_connection, 
#         response_id, 
#         request_data
#     )
#     assert response_data['valid'] == True, response_data

# def test_request_response_loop(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     plugin = plugins[0]
#     request_data = type_to_request_func[plugin['type']](plugin)

#     response_data = publish_and_poll(
#         redis_connection, 
#         rabbitmq_connection, 
#         request_data['request_data']['request_id'], 
#         request_data
#     )
#     assert response_data['valid'] == True

# def test_alt_queue(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     plugin = plugins[0]
#     request_data = type_to_request_func[plugin['type']](plugin)

#     # send to alt exchange
#     request_id = request_data['request_data']['request_id']
#     response_id = request_id.replace('request', 'blah')
#     request_data['request_data']['request_id'] = response_id

#     response_data = publish_and_poll(
#         redis_connection, 
#         rabbitmq_connection, 
#         response_id, 
#         request_data
#     )
#     assert response_data['valid'] == False 
#     assert response_data['failure_reason'] == 'Alt Ex'

# def test_dlx_queue(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     plugin = plugins[0]
#     request_data = type_to_request_func[plugin['type']](plugin)

#     # send to dead letter 
#     request_id = request_data['request_data']['request_id']
#     response_id = request_id.replace(plugin['type'], 'dlx_test')
#     request_data['request_data']['request_id'] = response_id

#     response_data = publish_and_poll(
#         redis_connection, 
#         rabbitmq_connection, 
#         response_id, 
#         request_data
#     )
#     assert response_data['valid'] == False 
#     assert response_data['failure_reason'] == 'Dead Letter'

# # Plugin type specific tests using the helpers
# def test_embedding_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     queue_request_helper(redis_connection, rabbitmq_connection, backend_client, plugins, 'embedding')

# def test_data_source_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     queue_request_helper(redis_connection, rabbitmq_connection, backend_client, plugins, 'data_source')

# def test_filter_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     queue_request_helper(redis_connection, rabbitmq_connection, backend_client, plugins, 'filter')

# def test_score_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     queue_request_helper(redis_connection, rabbitmq_connection, backend_client, plugins, 'score')

# def test_mapper_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     queue_request_helper(redis_connection, rabbitmq_connection, backend_client, plugins, 'mapper')

# def test_assembly_consumer(redis_connection, rabbitmq_connection, backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     queue_request_helper(redis_connection, rabbitmq_connection, backend_client, plugins, 'assembly')

# # Backend execution tests
# def test_embedding_backend_execution(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     result = execute_plugin_helper(backend_client, plugins, 'embedding', timeout=4)
#     assert 'result_id' not in result 
#     assert result['valid']

# def test_data_source_backend_execution(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     result = execute_plugin_helper(backend_client, plugins, 'data_source', timeout=4)
#     assert 'result_id' not in result 
#     assert result['valid']

# def test_filter_backend_execution(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     result = execute_plugin_helper(backend_client, plugins, 'filter', timeout=4)
#     assert 'result_id' not in result 
#     assert result['valid']

# def test_filter_backend_execution_batched(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     execute_plugin_helper(
#         backend_client, 
#         plugins, 
#         'filter', 
#         batched=True, 
#         batch_size=10, 
#         timeout=8
#     )

# def test_score_backend_execution(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     result = execute_plugin_helper(backend_client, plugins, 'score', timeout=4)
#     assert 'result_id' not in result 
#     assert result['valid']

# def test_mapper_backend_execution(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     result = execute_plugin_helper(backend_client, plugins, 'mapper', timeout=4)
#     assert 'result_id' not in result 
#     assert result['valid']

# def test_assembly_backend_execution(backend_client):
#     plugins = fetch_test_consumer_plugins(backend_client)
#     result = execute_plugin_helper(backend_client, plugins, 'assembly', timeout=4)
#     assert 'result_id' not in result 
#     assert result['valid']

