import pytest 

from tests.utils.backend_utils import backend_execute_plugin
from tests.utils.request_data import validate_api_response, get_plugin_and_request
from tests.utils.db_utils import (
    validate_execution_cache,
    validate_item_checkin,
    validate_data_source_checkin,
    validate_assembly_checkin
)

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
@pytest.mark.parametrize("batch_size", [1, 3])
async def test_backend_queue_execute(db_session, backend_client, plugin_type, batch_size):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        batch_size)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
async def test_backend_queue_execute_cache(db_session, backend_client, redis_connection, plugin_type):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type,
                                                        f"mock_{plugin_type}_queue_%", 
                                                        3)
    response = backend_execute_plugin(backend_client, request_data, 
                                      plugin['id'], params={'cache' : True})
    validate_api_response(plugin, response, 200)
    validate_execution_cache(redis_connection, request_data, plugin)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
async def test_backend_queue_execute_semaphore(db_session, backend_client, redis_connection, plugin_type):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type,
                                                        f"mock_{plugin_type}_queue_%", 
                                                        3)
    response = backend_execute_plugin(backend_client, request_data, 
                                      plugin['id'], params={'use_semaphore' : True})
    validate_api_response(plugin, response, 200)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding'])
@pytest.mark.parametrize("db_persist", [True, False])
async def test_backend_queue_execute_item_checkin(db_session, backend_client, plugin_type, db_persist):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        3)
    response_data = backend_execute_plugin(backend_client, request_data, 
                                           plugin['id'], params={'db_persist' : db_persist})
    validate_api_response(plugin, response_data, 200)
    await validate_item_checkin(db_session, request_data, response_data.json(), plugin, db_persist)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("db_persist", [True, False])
async def test_backend_queue_execute_data_source_checkin(db_session, backend_client, db_persist):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'data_source', 
                                                        f"mock_data_source_queue_%",
                                                        3)
    response_data = backend_execute_plugin(backend_client, request_data, 
                                           plugin['id'], params={'db_persist' : db_persist})

    validate_api_response(plugin, response_data, 200)
    await validate_data_source_checkin(db_session, response_data.json(), plugin, db_persist)
    await db_session.commit()

@pytest.mark.asyncio
async def test_backend_queue_execute_assembly_checkin(db_session, backend_client):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'assembly', 
                                                        f"mock_assembly_queue_%",
                                                        3)
    response_data = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response_data, 200)
    await validate_assembly_checkin(db_session, request_data, response_data.json(), plugin)
    await db_session.commit()

@pytest.mark.asyncio
async def test_backend_queue_execute_error(db_session, backend_client):
    plugin_type = 'filter'
    batch_size = 3
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        batch_size)
    request_data[0]['runtime_args'] = {'no_response' : True}
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)
    await db_session.commit()

@pytest.mark.asyncio
async def test_backend_queue_execute_item_error_checkin(db_session, backend_client):
    plugin_type = 'filter'
    db_persist = True 
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        3)
    
    request_data[0]['runtime_args'] = {'no_response' : True}
    response_data = backend_execute_plugin(backend_client, request_data, 
                                           plugin['id'], params={'db_persist' : db_persist})
    validate_api_response(plugin, response_data, 200)
    response_data = response_data.json()
    await validate_item_checkin(db_session, [request_data[0]], [response_data[0]], plugin, False)
    await validate_item_checkin(db_session, request_data[1:], response_data[1:], plugin, True)
    await db_session.commit()
