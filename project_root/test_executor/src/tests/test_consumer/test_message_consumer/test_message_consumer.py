# import pytest

# from tests.utils.request_data import get_plugin_and_request, validate_response
# from tests.utils.rabbitmq_utils import rabbitmq_publish, collect_replies

# @pytest.mark.asyncio
# @pytest.mark.parametrize("plugin_type", ["filter", "score", "embedding", "assembly", "mapper", "data_source"])
# async def test_message_consumer(
#     db_session,
#     rabbitmq_connection,
#     backend_client,
#     plugin_type,
# ):
#     batch_size = 3

#     # ── 1. build request objects ───────────────────────────────────
#     plugin, requests = await get_plugin_and_request(db_session, backend_client,
#                                                     plugin_type, f"mock_{plugin_type}_queue_%",
#                                                     batch_size, to_model=True)
#     conn, ch = rabbitmq_connection

#     # ── 2. create a private, auto-delete reply queue ───────────────
#     result       = ch.queue_declare(queue="", exclusive=True)
#     reply_queue  = result.method.queue

#     # ── 3. publish the batch and wait for all replies ──────────────
#     corr_ids = rabbitmq_publish(ch, requests, reply_queue)
#     replies  = collect_replies(conn, ch, reply_queue, corr_ids,
#                                interval=0.05, timeout=10.0)

#     validate_response(plugin, replies)

#     await db_session.commit()




# import pytest 
# from tests.utils.request_data import get_plugin_and_request, validate_response
# from tests.utils.rabbitmq_utils import rabbitmq_publish, poll_redis

# @pytest.mark.asyncio
# @pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
# async def test_message_consumer(db_session, rabbitmq_connection, redis_connection, backend_client, plugin_type):
#     batch_size = 3
#     plugin, request_data = await get_plugin_and_request(db_session, 
#                                                         backend_client, 
#                                                         plugin_type, 
#                                                         f"mock_{plugin_type}_queue_%",
#                                                         batch_size,
#                                                         to_model=True)
    
#     # changing `request` to `response` publishes directly to message consumer
#     for request in request_data:
#         request_id = request.request_data.request_id
#         response_id = request_id.replace('request', 'response')
#         request.request_data.request_id = response_id 

#     published = rabbitmq_publish(rabbitmq_connection, request_data)
#     response_keys = [i.replace('request', 'response').replace('.', ':') for i in published]
#     responses = poll_redis(redis_connection, response_keys, interval=0.05, timeout=10)
#     for response in responses:
#         assert response['valid'] == True, response 
#     await db_session.commit()

# @pytest.mark.asyncio
# @pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
# async def test_alt_queue(db_session, rabbitmq_connection, redis_connection, backend_client, plugin_type):
#     batch_size = 3
#     plugin, request_data = await get_plugin_and_request(db_session, 
#                                                         backend_client, 
#                                                         plugin_type, 
#                                                         f"mock_{plugin_type}_queue_%",
#                                                         batch_size,
#                                                         to_model=True)
    
#     # changing `request` to `blah` should route to alt queue
#     for request in request_data:
#         request_id = request.request_data.request_id
#         response_id = request_id.replace('request', 'blah')
#         request.request_data.request_id = response_id 

#     published = rabbitmq_publish(rabbitmq_connection, request_data)
#     response_keys = [i.replace('request', 'response').replace('.', ':') for i in published]
#     responses = poll_redis(redis_connection, response_keys, interval=0.05, timeout=10)
#     for response in responses:
#         assert response['valid'] == False, response 
#         assert response['failure_reason'] == 'Alt Ex', response 
#     await db_session.commit()

# @pytest.mark.asyncio
# @pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
# async def test_dlx_queue(db_session, rabbitmq_connection, redis_connection, backend_client, plugin_type):
#     batch_size = 3
#     plugin, request_data = await get_plugin_and_request(db_session, 
#                                                         backend_client, 
#                                                         plugin_type, 
#                                                         f"mock_{plugin_type}_queue_%",
#                                                         batch_size,
#                                                         to_model=True)
    
#     # changing plugin type to `dlx_test` should route to dlx queue
#     for request in request_data:
#         request_id = request.request_data.request_id
#         response_id = request_id.replace(plugin['type'], 'dlx_test')
#         request.request_data.request_id = response_id 

#     published = rabbitmq_publish(rabbitmq_connection, request_data)
#     response_keys = [i.replace('request', 'response').replace('.', ':') for i in published]
#     responses = poll_redis(redis_connection, response_keys, interval=0.05, timeout=10)
#     for response in responses:
#         assert response['valid'] == False, response 
#         assert response['failure_reason'] == 'Dead Letter', response 
#     await db_session.commit()

