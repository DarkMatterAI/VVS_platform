
from tests.utils import fetch_plugins_by_filter
from tests.test_helpers import execute_plugin_helper

def test_tei_ping(tei_client):
    response = tei_client.get('/info')
    assert response.status_code == 200

def test_tei_plugins_created(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, group_key='tei_plugin')
    assert len(plugins) > 0, "No TEI plugin found"

def test_backend_execution(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, group_key='tei_plugin')
    result = execute_plugin_helper(backend_client, plugins, plugins[0]['type'])
    assert result is not None

