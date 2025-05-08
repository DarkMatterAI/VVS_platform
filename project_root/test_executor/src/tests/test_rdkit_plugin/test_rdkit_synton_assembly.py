import pytest 

from tests.utils.request_data import generate_rdkit_assembly_request, validate_response, validate_api_response
from tests.utils.rabbitmq_utils import rabbitmq_publish, collect_replies # poll_redis
from tests.utils.backend_utils import backend_execute_plugin
from tests.utils.db_utils import validate_assembly_checkin

TEST_PARENTS = [
    ['O=P(NCc1ccc(Br)cc1)(Oc1ccccc1)Oc1ccccc1', 
     'CC(C)CCNCCO']
]

@pytest.mark.asyncio
async def test_rdkit_synton_assembly_consumer(db_session, rabbitmq_connection, redis_connection, 
                                              backend_client, synton_test_assembly):
    plugin = synton_test_assembly()
    request_data = await generate_rdkit_assembly_request(db_session, TEST_PARENTS, plugin, to_model=True)
    conn, ch = rabbitmq_connection
    result       = ch.queue_declare(queue="", exclusive=True)
    reply_queue  = result.method.queue

    corr_ids  = rabbitmq_publish(ch, request_data, reply_queue)
    responses = collect_replies(conn, ch, reply_queue, corr_ids,
                                interval=0.05, timeout=10.0)
    validate_response(plugin, responses)
    await db_session.commit()

@pytest.mark.asyncio
async def test_rdkit_assembly_backend(db_session, backend_client, synton_test_assembly):
    plugin = synton_test_assembly()
    request_data = await generate_rdkit_assembly_request(db_session, TEST_PARENTS, plugin)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)
    await validate_assembly_checkin(db_session, request_data, response.json(), plugin)
    await db_session.commit()

