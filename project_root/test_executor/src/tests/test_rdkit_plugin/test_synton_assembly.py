import itertools 
import pytest 

from tests.utils import publish_and_poll, get_request_id, delete_plugin
from tests.test_helpers import execute_plugin_helper
from vvs_database.schemas import PluginType

api_str = '/api/v1/rdkit_plugins'
plugin_api_str = '/api/v1/plugins'

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
def synton_test_assembly(backend_client):
    def _create_assembly():
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test SyntOn Assembly Integration {next(itertools.count(1))}",
                "plugin_class": "internal_rdkit",
                "type": "assembly",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "num_parents": 2,
                "config": {
                    'synt_on_reaction_stages': [{'step': 0, 'reactions': SYNTON_NAMES}]
                }
            }
        )
        assert response.status_code == 200
        return response.json()

    return _create_assembly

def get_request_data(plugin_record):
    request_data = {
        'request_id': None,
        "parents": [
            {"assembly_index": 0, "id": 1, "external_id": "1", "item": "O=P(NCc1ccc(Br)cc1)(Oc1ccccc1)Oc1ccccc1"},
            {"assembly_index": 1, "id": 2, "external_id": "2", "item": "CC(C)CCNCCO"},
        ]
    }
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def test_synton_smarts_assembly_consumer(redis_connection, rabbitmq_connection, synton_test_assembly, backend_client):
    assembly_record = synton_test_assembly()
    request_data = get_request_data(assembly_record)

    response_data = publish_and_poll(
        redis_connection, 
        rabbitmq_connection, 
        request_data['request_id'], 
        request_data
    )
    assert response_data['valid'] == True
    delete_plugin(assembly_record, backend_client, plugin_api_str)

def test_synton_smarts_assembly_backend_execution(backend_client, synton_test_assembly):
    assembly_record = synton_test_assembly()
    request_data = get_request_data(assembly_record)
    
    # Use helper with custom request data
    result = execute_plugin_helper(
        backend_client,
        [assembly_record],
        PluginType.ASSEMBLY,
        timeout=20,
        custom_request=request_data
    )
    
    assert 'result_id' not in result 
    delete_plugin(assembly_record, backend_client, plugin_api_str)

# import itertools 
# import pytest 
# import uuid 
# import time 

# from tests.utils import publish_and_poll, poll_backend, get_request_id, delete_plugin

# api_str = '/api/v1/rdkit_plugins'
# plugin_api_str = '/api/v1/plugins'

# SYNTON_NAMES = ["O-acylation", 
#                 "Olefination", 
#                 "Condensation_of_Y-NH2_with_carbonyl_compounds", 
#                 "Amine_sulphoacylation", 
#                 "C-C couplings", 
#                 "Radical_reactions", 
#                 "N-acylation", 
#                 "O-alkylation_arylation", 
#                 "Metal organics C-C bong assembling", 
#                 "S-alkylation_arylation", 
#                 "Alkylation_arylation_of_NH-lactam", 
#                 "Alkylation_arylation_of_NH-heterocycles", 
#                 "Amine_alkylation_arylation"
#                 ]

# @pytest.fixture(scope="function")
# def synton_test_assembly(backend_client):
#     def _create_assembly():
#         response = backend_client.post(
#             f"{api_str}/",
#             json={
#                 "name": f"Test SyntOn Assembly Integration {next(itertools.count(1))}",
#                 "plugin_class" : "internal_rdkit",
#                 "type": "assembly",
#                 "execution_type": "queue",
#                 "group_key": "rdkit_plugin",
#                 "timeout": 30,
#                 "max_concurrency": 5,
#                 "max_retries": 1,
#                 "num_parents": 2,
#                 "config": {
#                     'synt_on_reaction_stages' : [{'step' : 0, 'reactions' : SYNTON_NAMES}]
#                 }
#             }
#         )
#         assert response.status_code == 200
#         return response.json()

#     return _create_assembly

# def get_request_data(plugin_record):
#     request_data = {
#         'request_id' : None,
#         "parents": [
#             {"assembly_index" : 0, "id" : 1, "external_id" : "1", "item" : "O=P(NCc1ccc(Br)cc1)(Oc1ccccc1)Oc1ccccc1"},
#             {"assembly_index" : 1, "id" : 2, "external_id" : "2", "item" : "CC(C)CCNCCO"},
#         ]
#     }
#     request_data['request_id'] = get_request_id(plugin_record)
#     return request_data 

# def test_synton_smarts_assembly_consumer(redis_connection, rabbitmq_connection, synton_test_assembly, backend_client):
#     assembly_record = synton_test_assembly()
#     request_data = get_request_data(assembly_record)

#     response_data = publish_and_poll(redis_connection, rabbitmq_connection, request_data['request_id'], request_data)
#     assert response_data['valid'] == True
#     delete_plugin(assembly_record, backend_client, plugin_api_str)

# def test_synton_smarts_assembly_backend_execution(backend_client, synton_test_assembly):
#     assembly_record = synton_test_assembly()
#     request_data = get_request_data(assembly_record)

#     response = backend_client.post(f"/api/v1/execute/{assembly_record['id']}", json=request_data)
#     assert response.status_code == 200
#     result_id = response.json()['result_id']

#     result = poll_backend(backend_client, result_id, timeout=20)

#     assert 'result_id' not in result 
#     delete_plugin(assembly_record, backend_client, plugin_api_str)


