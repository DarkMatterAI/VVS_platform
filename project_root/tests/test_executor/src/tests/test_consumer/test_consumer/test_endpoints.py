import pytest 
from tests.utils.request_data import get_plugin_and_request, validate_response
from tests.utils.rabbitmq_utils import rabbitmq_publish, collect_replies #, poll_redis


@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
@pytest.mark.parametrize("batch_size", [1, 3, 10])
async def test_consumer_endpoint(db_session,
                                 rabbitmq_connection,  # ‹— fixture returns (connection, channel)
                                 backend_client,
                                 plugin_type,
                                 batch_size):

    conn, ch = rabbitmq_connection
    # ------------------------------------------------------------------
    # 1. set up a private reply queue
    result       = ch.queue_declare(queue="", exclusive=True)
    reply_queue  = result.method.queue
    # ------------------------------------------------------------------
    # 2. create request objects
    plugin, reqs = await get_plugin_and_request(
        db_session, backend_client, plugin_type,
        f"mock_{plugin_type}_queue_%", batch_size, to_model=True
    )
    # ------------------------------------------------------------------
    # 3. publish and wait for replies
    corr_ids  = rabbitmq_publish(ch, reqs, reply_queue)
    responses = collect_replies(conn, ch, reply_queue, corr_ids,
                                interval=0.05, timeout=10.0)
    # ------------------------------------------------------------------
    # 4. assertions
    validate_response(plugin, responses)
    await db_session.commit()
