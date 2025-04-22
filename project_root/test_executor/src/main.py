import yaml
import pytest
import sys
import time 
from pathlib import Path

TEST_ROOT = 'tests'

def load_config():
    with open('test_config.yaml', 'r') as file:
        test_config = yaml.safe_load(file)

    with open('launch_config.yaml', 'r') as file:
        plugin_config = yaml.safe_load(file)

    return test_config, plugin_config

def get_enabled_tests(config):
    test_paths = []
    for test, details in config.get('temp_services', {}).items():
        if (details.get('enabled', False) and 
            details.get('run_tests', False)):
            test_path = Path(f'{TEST_ROOT}/{test}')
            if test_path.exists():
                test_paths.append(str(test_path))
    return test_paths 

def get_enabled_plugin_tests(plugin_config, test_config):
    test_paths = []
    for plugin, details in test_config.get('services', {}).items():
        if (details.get('integration_test', False) and 
            plugin_config.get('plugins', {}).get(plugin, {}).get('enabled', False)):
            print(f"Adding tests for {plugin}")
            test_path = Path(f"{TEST_ROOT}/test_{plugin}")
            if test_path.exists():
                test_paths.append(str(test_path))
    return test_paths 

def get_enabled_job_tests(config):
    tmp_service_enabled = False 
    for test, details in config.get('temp_services', {}).items():
        if details.get('enabled', False):
            tmp_service_enabled = True 

    test_paths = []
    if not tmp_service_enabled:
        return test_paths 
    
    for job, details in config.get('jobs', {}).items():
        if details.get('run_tests', False):
            test_path = Path(f"{TEST_ROOT}/test_jobs/test_{job}")
            test_paths.append(test_path)
    return test_paths 

def main():
    print('Starting Integration tests')
    print('Sleeping while test containers setup')
    time.sleep(3)
    test_config, plugin_config = load_config()

    test_paths = [f'{TEST_ROOT}/test_s3', f'{TEST_ROOT}/test_redis']
    test_paths += get_enabled_tests(test_config)
    test_paths += get_enabled_plugin_tests(plugin_config, test_config)
    test_paths += get_enabled_job_tests(test_config)
    print(test_paths)

    if not test_paths:
        print("No enabled tests found.")
        sys.exit(0)

    exit_code = pytest.main(test_paths)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()

