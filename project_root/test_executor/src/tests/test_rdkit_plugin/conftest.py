
import itertools 
import pytest 
import uuid 
import string 
import numpy as np 


api_str = '/api/v1/rdkit_plugins'
plugin_api_str = '/api/v1/plugins'

_filter_counter = itertools.count(1)
_assembly_counter = itertools.count(1)
random_rdkit_key = ''.join(np.random.choice([i for i in string.ascii_lowercase], 8))

SYNTON_NAMES = [
    "O-acylation", 
    "Olefination", 
    "Condensation_of_Y-NH2_with_carbonyl_compounds", 
    "Amine_sulphoacylation", 
    "C-C couplings", 
    "Radical_reactions", 
    "N-acylation", 
    "O-alkylation_arylation", 
    "Metal organics C-C bong assembling", 
    "S-alkylation_arylation", 
    "Alkylation_arylation_of_NH-lactam", 
    "Alkylation_arylation_of_NH-heterocycles", 
    "Amine_alkylation_arylation"
]

@pytest.fixture(scope="function")
def rdkit_test_filter(backend_client, plugin_cleanup):
    def _create_filter():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test RDKit Filter Integration {next(_filter_counter)} {random_rdkit_key}",
                "plugin_class": "internal_rdkit",
                "type": "filter",
                "execution_type": "queue",
                "group_key": "rdkit_plugin_filter",
                "timeout": 30,
                "max_concurrency": 12,
                "max_retries": 1,
                "config": {"property_filters": [{"property_name": "TPSA", "min_val": 10}]}
            }
        )
        assert response.status_code == 200
        response = response.json()
        plugin_cleanup(response)
        return response 

    return _create_filter

@pytest.fixture(scope="function")
def rdkit_test_assembly(backend_client, plugin_cleanup):
    def _create_assembly():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test RDKit Assembly Integration {_assembly_counter} {random_rdkit_key}",
                "plugin_class": "internal_rdkit",
                "type": "assembly",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 12,
                "max_retries": 1,
                "num_parents": 2,
                "config": {
                    'single_stage_reactions': [{'smarts': '[C:1].[N:2]>>[C:1]-[N:2]', 'requires_hs': False}],
                    'multi_stage_reactions': []
                }
            }
        )
        assert response.status_code == 200
        response = response.json()
        plugin_cleanup(response)
        return response 

    return _create_assembly

@pytest.fixture(scope="function")
def synton_test_assembly(backend_client, plugin_cleanup):
    def _create_assembly():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test SyntOn Assembly Integration {_assembly_counter} {random_rdkit_key}",
                "plugin_class": "internal_rdkit",
                "type": "assembly",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 12,
                "max_retries": 1,
                "num_parents": 2,
                "config": {
                    'synt_on_reaction_stages': [{'step': 0, 'reactions': SYNTON_NAMES}]
                }
            }
        )
        assert response.status_code == 200
        response = response.json()
        plugin_cleanup(response)
        return response 

    return _create_assembly

