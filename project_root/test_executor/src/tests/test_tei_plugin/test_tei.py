
from tests.utils import fetch_plugins_by_filter, type_to_request_func


def test_tei_ping(tei_client):
    response = tei_client.get('/info')
    assert response.status_code == 200

def test_tei_plugins_created(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, group_key='tei_plugin')
    assert len(plugins) > 0, "No TEI plugin found"

def test_backend_execution(backend_client):
    plugin = fetch_plugins_by_filter(backend_client, group_key='tei_plugin')[0]
    request_data = type_to_request_func[plugin['type']](plugin)
    response = backend_client.post(f"/api/v1/execute/{plugin['id']}", json=request_data)
    assert response.status_code == 200
