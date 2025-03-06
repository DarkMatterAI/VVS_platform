import pytest 

from tests_new.utils.backend_utils import backend_get_plugins_by_filter
from tests_new.utils.request_data import validate_api_response, get_plugin_and_request
from tests_new.utils.backend_utils import backend_execute_plugin


def test_tei_ping(tei_client):
    response = tei_client.get('/info')
    assert response.status_code == 200

def test_tei_plugins_created(backend_client):
    plugins = backend_get_plugins_by_filter(backend_client, group_key='tei_plugin')
    assert len(plugins) > 0, "No TEI plugin found"

@pytest.mark.asyncio
async def test_backend_tei_execute(db_session, backend_client):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        'embedding', 
                                                        group_key='tei_plugin',
                                                        batch_size=3)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)


