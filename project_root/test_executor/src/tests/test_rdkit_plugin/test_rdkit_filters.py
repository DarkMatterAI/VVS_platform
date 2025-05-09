import pytest 

from tests.utils.request_data import (
    generate_rdkit_item_request, 
    validate_response, 
    validate_api_response,
)
from tests.utils.rabbitmq_utils import rabbitmq_publish, collect_replies # poll_redis
from tests.utils.backend_utils import backend_execute_plugin
from tests.utils.db_utils import validate_item_checkin


TEST_SMILES = ['CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1']

@pytest.mark.asyncio
async def test_rdkit_filter_consumer(db_session, rabbitmq_connection, rdkit_test_filter):
    plugin = rdkit_test_filter()
    request_data = await generate_rdkit_item_request(db_session, TEST_SMILES, plugin, to_model=True)
    conn, ch = rabbitmq_connection
    result       = ch.queue_declare(queue="", exclusive=True)
    reply_queue  = result.method.queue

    corr_ids  = rabbitmq_publish(ch, request_data, reply_queue)
    responses = collect_replies(conn, ch, reply_queue, corr_ids,
                                interval=0.05, timeout=10.0)
    validate_response(plugin, responses)
    await db_session.commit()

@pytest.mark.asyncio
async def test_rdkit_filter_backend(db_session, backend_client, rdkit_test_filter):
    plugin = rdkit_test_filter()
    request_data = await generate_rdkit_item_request(db_session, TEST_SMILES, plugin)
    response = backend_execute_plugin(backend_client, request_data, 
                                      plugin['id'], params={'db_persist' : True})
    validate_api_response(plugin, response, 200)
    await validate_item_checkin(db_session, request_data, response.json(), plugin, True)
    await db_session.commit()

