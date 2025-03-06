import pytest 
from tests.utils.request_data import get_plugin_and_request, validate_response
from tests.utils.rabbitmq_utils import rabbitmq_publish, poll_redis

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
@pytest.mark.parametrize("batch_size", [1, 3, 10])
async def test_consumer_endpoint(db_session, rabbitmq_connection, redis_connection, backend_client, plugin_type, batch_size):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        batch_size,
                                                        to_model=True)
    published = rabbitmq_publish(rabbitmq_connection, request_data)
    response_keys = [i.replace('request', 'response').replace('.', ':') for i in published]
    response = poll_redis(redis_connection, response_keys, interval=0.05, timeout=10)
    response = [i['response_data'] for i in response]
    validate_response(plugin, response)

