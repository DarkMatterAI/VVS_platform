import pytest 

from tests.utils.request_data import get_plugin_and_request

from vvs_database.schemas import ExecuteParams, PluginInDB, EmbedResponse, ExecutionSources
from vvs_database.execution.connections import get_connections
from vvs_database.execution.execution_strategy import QueueExecutionStrategy

@pytest.mark.asyncio
async def test_db_semaphore_failure(db_session, backend_client, monkeypatch):
    """
    APIExecutionStrategy should mark every request invalid when it cannot
    obtain a semaphore token within `max_semaphore_attempts`.
    """
    # -------- 1. discover one mock EMBEDDING plugin + sample requests ------
    plugin_type = "embedding"
    plugin_rec, req_data = await get_plugin_and_request(
        db_session,
        backend_client,
        plugin_type,
        f"mock_{plugin_type}_queue_%",   # glob pattern used in your helpers
        1,                             # how many plugins
        to_model=True,
    )
    plugin = PluginInDB(**plugin_rec)

    # -------- 2. Strategy under test ---------------------------------------
    connections = get_connections(db_session)

    execute_params = ExecuteParams(
        cache=False,
        db_lookup=False,
        db_persist=False,
        use_semaphore=True,
        max_semaphore_attempts=1,      # fail after first empty acquisition
    )
    executor = QueueExecutionStrategy(connections, execute_params)
    redis_service = connections.redis_service

    # -------- 3. Monkey‑patch semaphore acquisition to *always* fail -------
    async def fake_acquire_batch(*args, **kwargs):
        return []                       # zero identifiers → failure path
    monkeypatch.setattr(redis_service, "acquire_semaphores_batch", fake_acquire_batch)

    # (release_semaphore will be called with empty list; leave it unchanged)

    # -------- 4. Build request_dict & execute ------------------------------
    request_dict = {r.generate_key(plugin_id=plugin.id): r for r in req_data}

    response = await executor.execute(plugin, request_dict)

    # -------- 5. All responses must be marked invalid ----------------------
    assert response, "executor returned empty response"
    for key, val in response.items():
        assert val.valid is False, f"key {key} unexpectedly succeeded"
        assert val.failure_reason == "Semaphore failure"


    await connections.close()


@pytest.mark.asyncio
async def test_db_aggressive_cache(db_session, backend_client):
    """
    APIExecutionStrategy should return a result with `source="cache"` if 
    the request exists in cache and `aggressive_cache=True`
    """
    # -------- 1. discover one mock EMBEDDING plugin + sample requests ------
    plugin_type = "embedding"
    plugin_rec, req_data = await get_plugin_and_request(
        db_session,
        backend_client,
        plugin_type,
        f"mock_{plugin_type}_queue_%",
        1, 
        to_model=True,
    )
    plugin = PluginInDB(**plugin_rec)

    # -------- 2. Strategy under test ---------------------------------------
    connections = get_connections(db_session)

    execute_params = ExecuteParams(
        cache=True,
        aggressive_cache=True,
        db_lookup=False,
        db_persist=False,
        use_semaphore=True,
        max_semaphore_attempts=10,
    )
    executor = QueueExecutionStrategy(connections, execute_params)
    redis_service = connections.redis_service

    # -------- 3. Set key in cache ------------------------------------------
    request_dict = {}
    response_dict = {}
    for i, r in enumerate(req_data):
        cache_key = r.generate_key(plugin_id=plugin.id)
        request_dict[cache_key] = r
        response_dict[cache_key] = EmbedResponse(valid=True, embedding=[float(i) for i in range(3)])
    await redis_service.set_results(response_dict)

    # -------- 4. Execute ---------------------------------------------------
    response = await executor.execute(plugin, request_dict)

    # -------- 5. All responses must be marked invalid ----------------------
    assert response, "executor returned empty response"
    for key, val in response.items():
        assert val.valid, f"key {key} unexpectedly failed"
        assert val.source == ExecutionSources.CACHE
        assert val.response_data == response_dict[key].model_dump()

    await connections.close()

