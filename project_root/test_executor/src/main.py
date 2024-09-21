import yaml
import pytest
import sys
import time 
from pathlib import Path

def load_config():
    with open('test_config.yaml', 'r') as file:
        test_config = yaml.safe_load(file)

    with open('launch_config.yaml', 'r') as file:
        plugin_config = yaml.safe_load(file)

    return test_config, plugin_config

def get_enabled_tests(config):
    test_paths = []
    for test, details in config.get('integration_tests', {}).items():
        if details.get('enabled', False):
            test_path = Path(f'tests/{test}')
            if test_path.exists():
                test_paths.append(str(test_path))
    return test_paths

def get_enabled_plugin_tests(plugin_config, test_config):
    test_paths = []
    for plugin, details in test_config.get('plugin_integration_tests', {}).items():
        print(plugin)
        if (details.get('enabled', False) and 
            plugin_config.get('plugins', {}).get(plugin, {}).get('enabled', False)):
            test_path = Path(f"tests/test_{plugin}")
            if test_path.exists():
                test_paths.append(str(test_path))
    return test_paths 


def main():
    print('Starting Integration tests')
    print('Sleeping while test containers setup')
    time.sleep(3)
    test_config, plugin_config = load_config()

    test_paths = get_enabled_tests(test_config)
    test_paths += get_enabled_plugin_tests(plugin_config, test_config)
    print(test_paths)

    if not test_paths:
        print("No enabled tests found.")
        sys.exit(0)

    exit_code = pytest.main(test_paths)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()



# import yaml
# import pytest
# import sys
# import time 
# from pathlib import Path

# def load_config():
#     with open('test_config.yaml', 'r') as file:
#         test_config = yaml.safe_load(file)

#     with open('launch_config.yaml', 'r') as file:
#         plugin_config = yaml.safe_load(file)

#     return test_config, plugin_config

# def get_enabled_tests(config):
#     enabled_tests = []
#     for test, details in config.get('integration_tests', {}).items():
#         if details.get('enabled', False):
#             enabled_tests.append(test)
#     return enabled_tests

# def main():
#     print('Starting Integration tests')
#     print('Sleeping while test containers setup')
#     time.sleep(3)
#     test_config, plugin_config = load_config()
#     enabled_tests = get_enabled_tests(test_config)
#     print(enabled_tests)

#     test_paths = []
#     for test in enabled_tests:
#         test_path = Path(f'tests/{test}')
#         if test_path.exists():
#             test_paths.append(str(test_path))

#     if not test_paths:
#         print("No enabled tests found.")
#         sys.exit(0)

#     exit_code = pytest.main(test_paths)
#     sys.exit(exit_code)

# if __name__ == "__main__":
#     main()