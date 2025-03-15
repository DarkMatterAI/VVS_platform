import pytest 

from tests.utils.request_data import validate_response, get_plugin_and_request
from tests.utils.db_utils import (
    validate_execution_cache,
    validate_item_checkin,
    validate_data_source_checkin,
    validate_assembly_checkin
)

from vvs_database.execution import execute_plugin
from vvs_database.schemas import ExecuteParams

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
@pytest.mark.parametrize("batch_size", [1, 3])
async def test_db_queue_execute(db_session, backend_client, plugin_type, batch_size):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        batch_size, 
                                                        to_model=True)
    if batch_size == 1:
        request_data = request_data[0]

    execute_params = ExecuteParams(cache=False, db_lookup=False, db_persist=False, use_semaphore=False)
    response = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    
    validate_response(plugin, response)

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
async def test_db_queue_execute_cache(db_session, backend_client, redis_connection, plugin_type):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        3, 
                                                        to_model=True)

    execute_params = ExecuteParams(cache=True, db_lookup=False, db_persist=False, use_semaphore=False)
    response = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response)
    validate_execution_cache(redis_connection, request_data, plugin)

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
async def test_db_queue_execute_semaphore(db_session, backend_client, redis_connection, plugin_type):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        3, 
                                                        to_model=True)

    execute_params = ExecuteParams(cache=True, db_lookup=False, db_persist=False, use_semaphore=True)
    response = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response)

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding'])
@pytest.mark.parametrize("db_persist", [True, False])
async def test_db_queue_execute_item_checkin(db_session, backend_client, plugin_type, db_persist):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        3, 
                                                        to_model=True)
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=db_persist, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)
    await validate_item_checkin(db_session, request_data, response_data, plugin, db_persist)

@pytest.mark.asyncio
@pytest.mark.parametrize("db_persist", [True, False])
async def test_db_queue_execute_data_source_checkin(db_session, backend_client, db_persist):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'data_source', 
                                                        f"mock_data_source_queue_%",
                                                        3, 
                                                        to_model=True)
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=db_persist, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)
    await validate_data_source_checkin(db_session, response_data, plugin, db_persist)

@pytest.mark.asyncio
async def test_db_queue_execute_assembly_checkin(db_session, backend_client):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'assembly', 
                                                        f"mock_assembly_queue_%",
                                                        3, 
                                                        to_model=True)
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=False, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)
    await validate_assembly_checkin(db_session, request_data, response_data, plugin)

@pytest.mark.asyncio
async def test_db_queue_execute_error(db_session, backend_client):
    plugin_type = 'filter'
    batch_size = 3
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        batch_size, 
                                                        to_model=True)
    request_data[1].runtime_args = {'no_response' : True}

    execute_params = ExecuteParams(cache=False, db_lookup=False, db_persist=False, use_semaphore=False)
    response, checkin_response, valid_execution = await execute_plugin(db_session, plugin['id'], request_data, 
                                                                       execute_params, return_all=True)
    assert valid_execution[1] == False, valid_execution
    
    validate_response(plugin, response)

@pytest.mark.asyncio
async def test_db_queue_execute_item_error_checkin(db_session, backend_client):
    plugin_type = 'filter'
    db_persist = True 
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_queue_%",
                                                        3, 
                                                        to_model=True)
    request_data[0].runtime_args = {'no_response' : True}
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=db_persist, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)

    # error should prevent db persist 
    await validate_item_checkin(db_session, [request_data[0]], [response_data[0]], plugin, False)
    await validate_item_checkin(db_session, request_data[1:], response_data[1:], plugin, True)

