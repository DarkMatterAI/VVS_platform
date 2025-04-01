import pytest 

from tests.utils.request_data import (
    generate_rdkit_item_request, 
    validate_response, 
    validate_api_response,
)
from tests.utils.rabbitmq_utils import rabbitmq_publish, poll_redis
from tests.utils.backend_utils import backend_execute_plugin
from tests.utils.db_utils import validate_item_checkin


TEST_SMILES = ['CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1']

@pytest.mark.asyncio
async def test_rdkit_score_consumer(db_session, rabbitmq_connection, redis_connection, 
                                     backend_client, rdkit_test_score):
    plugin = rdkit_test_score()
    request_data = await generate_rdkit_item_request(db_session, TEST_SMILES, plugin, to_model=True)
    published = rabbitmq_publish(rabbitmq_connection, request_data)
    response_keys = [i.replace('request', 'response').replace('.', ':') for i in published]
    response = poll_redis(redis_connection, response_keys, interval=0.05, timeout=10)
    response = [i['response_data'] for i in response]
    validate_response(plugin, response)

@pytest.mark.asyncio
async def test_rdkit_score_backend(db_session, backend_client, rdkit_test_score):
    plugin = rdkit_test_score()
    request_data = await generate_rdkit_item_request(db_session, TEST_SMILES, plugin)
    response = backend_execute_plugin(backend_client, request_data, 
                                      plugin['id'], params={'db_persist' : True})
    validate_api_response(plugin, response, 200)
    await validate_item_checkin(db_session, request_data, response.json(), plugin, True)

