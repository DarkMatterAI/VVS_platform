import pytest 

from tests.utils.request_data import validate_response, get_plugin_and_request
from tests.utils.db_utils import (
    validate_execution_cache,
    validate_item_checkin,
    validate_data_source_checkin,
    validate_assembly_checkin,
    get_execution_failures
)

from vvs_database.execution import execute_plugin
from vvs_database.schemas import ExecuteParams

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
@pytest.mark.parametrize("batch_size", [1, 3, 10])
async def test_db_api_execute(db_session, backend_client, plugin_type, batch_size):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%",
                                                        batch_size, 
                                                        to_model=True)
    if batch_size == 1:
        request_data = request_data[0]

    execute_params = ExecuteParams(cache=False, db_lookup=False, db_persist=False, use_semaphore=False)
    response = await execute_plugin(db_session, plugin['id'], request_data, execute_params)    
    validate_response(plugin, response)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
async def test_db_api_execute_cache(db_session, backend_client, redis_connection, plugin_type):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%",
                                                        3, 
                                                        to_model=True)

    execute_params = ExecuteParams(cache=True, db_lookup=False, db_persist=False, use_semaphore=False)
    response = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response)
    validate_execution_cache(redis_connection, request_data, plugin)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
async def test_db_api_execute_semaphore(db_session, backend_client, redis_connection, plugin_type):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%",
                                                        3, 
                                                        to_model=True)

    execute_params = ExecuteParams(cache=False, db_lookup=False, db_persist=False, use_semaphore=True)
    response = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding'])
@pytest.mark.parametrize("db_persist", [True, False])
async def test_db_api_execute_item_checkin(db_session, backend_client, plugin_type, db_persist):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%",
                                                        3, 
                                                        to_model=True)
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=db_persist, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)
    await validate_item_checkin(db_session, request_data, response_data, plugin, db_persist)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("db_persist", [True, False])
async def test_db_api_execute_data_source_checkin(db_session, backend_client, db_persist):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'data_source', 
                                                        f"mock_data_source_api_%",
                                                        3, 
                                                        to_model=True)
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=db_persist, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)
    await validate_data_source_checkin(db_session, response_data, plugin, db_persist)
    await db_session.commit()

@pytest.mark.asyncio
async def test_db_api_execute_assembly_checkin(db_session, backend_client):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'assembly', 
                                                        f"mock_assembly_api_%",
                                                        3, 
                                                        to_model=True)
    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=False, use_semaphore=False)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)
    await validate_assembly_checkin(db_session, request_data, response_data, plugin)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'data_source', 'assembly', 'mapper'])
async def test_db_api_execution_error(db_session, backend_client, plugin_type):
    batch_size = 1
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%",
                                                        batch_size, 
                                                        to_model=True)
    request_data = request_data[0]
    request_data.runtime_args = {'throw_error' : True}

    initial_errors = await get_execution_failures(db_session, plugin['id'])

    execute_params = ExecuteParams(cache=False, db_lookup=False, db_persist=False, use_semaphore=False)
    response, checkin_response, valid_execution = await execute_plugin(db_session, plugin['id'], request_data, 
                                                                       execute_params, return_all=True)
    assert valid_execution[0] == False, valid_execution

    final_errors = await get_execution_failures(db_session, plugin['id'])
    assert final_errors > initial_errors
    await db_session.commit()

@pytest.mark.asyncio
async def test_db_api_execute_item_error_checkin(db_session, backend_client):
    plugin_type = 'filter'
    db_persist = True 
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%",
                                                        3, 
                                                        to_model=True)
    request_data[0].runtime_args = {'throw_error' : True}

    initial_errors = await get_execution_failures(db_session, plugin['id'])

    execute_params = ExecuteParams(cache=False, db_lookup=True, db_persist=False, use_semaphore=db_persist)
    response_data = await execute_plugin(db_session, plugin['id'], request_data, execute_params)
    validate_response(plugin, response_data)

    # error should prevent db persist 
    await validate_item_checkin(db_session, request_data, response_data, plugin, False)

    final_errors = await get_execution_failures(db_session, plugin['id'])
    assert final_errors > initial_errors
    await db_session.commit()
