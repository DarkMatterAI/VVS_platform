import pytest 

from tests_new.utils.backend_utils import backend_get_plugins_by_filter
from tests_new.utils.request_data import validate_api_response, get_plugin_and_request
from tests_new.utils.backend_utils import backend_execute_plugin

def test_triton_ping(triton_client):
    response = triton_client.get('/v2/health/live')
    assert response.status_code == 200

def test_triton_embedding_plugins_created(backend_client):
    plugins = backend_get_plugins_by_filter(backend_client, group_key='triton_plugin_embedding')
    assert len(plugins) == 6, "Incorrect number of triton embedding plugins"

def test_triton_mapper_plugins_created(backend_client):
    plugins = backend_get_plugins_by_filter(backend_client, group_key='triton_plugin_mapper')
    assert len(plugins) == 1, "Incorrect number of triton embedding plugins"

@pytest.mark.asyncio
async def test_backend_triton_embed_execute(db_session, backend_client):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'embedding', 
                                                        group_key='triton_plugin_embedding',
                                                        batch_size=3)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)

@pytest.mark.asyncio
async def test_backend_triton_mapper_execute(db_session, backend_client):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'mapper', 
                                                        group_key='triton_plugin_mapper',
                                                        batch_size=3)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)
