import yaml
import pytest
import sys
from pathlib import Path

def load_plugin_config():
    with open('app/launch_config.yaml', 'r') as file:
        return yaml.safe_load(file)

def get_enabled_plugins(config):
    enabled_plugins = []
    for plugin, details in config.get('plugins', {}).items():
        if details.get('enabled', False):
            print(f"Plugin {plugin} enabled - adding to test")
            enabled_plugins.append(plugin)
        else:
            print(f"Plugin {plugin} not enabled - skipping")
    return enabled_plugins

if __name__ == "__main__":
    print('Starting Backend Tests')

    print('Loading plugin config')
    config = load_plugin_config()

    enabled_plugins = get_enabled_plugins(config)

    test_paths = [Path(f'tests/core_tests')]

    for plugin in enabled_plugins:
        test_path = Path(f'tests/plugin_tests/')
        if test_path.exists():
            test_paths.append(test_path)
        else:
            print(f"Test path for {plugin} not found - skipping")

    exit_code = pytest.main(test_paths)
    sys.exit(exit_code)

