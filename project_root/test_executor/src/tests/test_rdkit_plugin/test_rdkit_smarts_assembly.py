
import pytest 

from tests.utils.request_data import generate_rdkit_assembly_request, validate_response, validate_api_response
from tests.utils.rabbitmq_utils import rabbitmq_publish, poll_redis
from tests.utils.backend_utils import backend_execute_plugin
from tests.utils.db_utils import validate_assembly_checkin

TEST_PARENTS = [
    ['C', 'N']
]

@pytest.mark.asyncio
async def test_rdkit_smarts_assembly_consumer(db_session, rabbitmq_connection, redis_connection, 
                                     backend_client, rdkit_test_assembly):
    plugin = rdkit_test_assembly()
    request_data = await generate_rdkit_assembly_request(db_session, TEST_PARENTS, plugin, to_model=True)
    published = rabbitmq_publish(rabbitmq_connection, request_data)
    response_keys = [i.replace('request', 'response').replace('.', ':') for i in published]
    response = poll_redis(redis_connection, response_keys, interval=0.05, timeout=10)
    response = [i['response_data'] for i in response]
    response = validate_response(plugin, response)
    await db_session.commit()

@pytest.mark.asyncio
async def test_rdkit_assembly_backend(db_session, backend_client, rdkit_test_assembly):
    plugin = rdkit_test_assembly()
    request_data = await generate_rdkit_assembly_request(db_session, TEST_PARENTS, plugin)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)
    await validate_assembly_checkin(db_session, request_data, response.json(), plugin)
    await db_session.commit()


