import os 
import pytest 
from tests.utils.request_data import validate_api_response, get_plugin_and_request

MAX_BATCH_SIZE = int(os.environ.get('TEST_BATCH_SIZE', 5))

def test_server_ping(test_api_client):
    response = test_api_client.get('/')
    assert response.status_code == 200 

@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_type", ['filter', 'score', 'embedding', 'assembly', 'mapper', 'data_source'])
@pytest.mark.parametrize("batch_size", [1, 3, 10])
async def test_server_endpoint(db_session, test_api_client, backend_client, plugin_type, batch_size):
    plugin, request_data = await get_plugin_and_request(db_session, 
                                                        backend_client, 
                                                        plugin_type, 
                                                        f"mock_{plugin_type}_api_%", 
                                                        batch_size)

    if batch_size == 1:
        request_data = request_data[0]

    response = test_api_client.post(f"/{plugin_type}", json=request_data)

    if batch_size > MAX_BATCH_SIZE:
        status_code = 422 
    else:
        status_code = 200

    validate_api_response(plugin, response, status_code)
