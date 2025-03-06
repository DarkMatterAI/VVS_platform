from tests_new.utils.backend_utils import backend_get_plugins_by_filter

def test_plugins_created(backend_client):
    target_counts = {
        'embedding': 3,
        'data_source': 1,
        'filter': 1,
        'score': 1,
        'mapper': 1,
        'assembly': 1
    }

    plugins = backend_get_plugins_by_filter(backend_client, "mock_%_queue_%")
    plugin_counts = {}
    for plugin in plugins:
        for s in ['mock', 'queue']:
            assert s in plugin['name'], f"Unexpected plugin name: {plugin['name']}"
        plugin_counts[plugin['type']] = plugin_counts.get(plugin['type'], 0) + 1

    for k,v in target_counts.items():
        assert plugin_counts.get(k,0) == v, f"Expected {v} plugins of type {k}, got {plugin_counts.get(k, 0)}"

