from tests.utils import fetch_plugins_by_filter, get_request_id, poll_backend, publish_and_poll

reaction_data = [
    {'id': 22, 'r1': 'CS(=O)(=O)c1ccc(CCN)cc1', 'r2': 'O=C(O)c1cc(C(F)(F)F)n[nH]1'},
    {'id': 11, 'r1': 'NNC(=O)c1ccccc1', 'r2': 'O=C(O)C(F)c1ccc(F)cc1'},
    {'id': 527, 'r1': 'Cc1cc(CCN)c2cn[nH]c2c1', 'r2': 'Cc1cccc(Nc2ccccc2C(=O)O)c1C'},
    {'id': 240690, 'r1': 'CON(C)C1CCNCC1', 'r2': 'O=C(O)c1cccc(CC(F)(F)F)c1'},
    {'id': 2430, 'r1': 'CCc1cccc(N)c1CC', 'r2': 'Cc1cc(F)c(C)cc1N'},
    {'id': 2708, 'r1': 'CC(N)Cc1ccc(Cl)s1', 'r2': 'CC1CC(N)CS1'},
    {'id': 2230, 'r1': 'CNCc1ccc2[nH]nnc2c1', 'r2': 'CC(C)Oc1cnc(Cl)nc1'},
    {'id': 2718, 'r1': 'Cn1nc2c(c1N)CCC2', 'r2': 'NCCOCC(F)F'},
    {'id': 40, 'r1': 'NCC12CC(CN1Cc1ccccc1)C2', 'r2': 'O=S(=O)(Cl)CCCC1CCOCC1'},
    {'id': 27, 'r1': 'Nc1nc2nc(Cl)ccc2s1', 'r2': 'CC(C)(C)OC(=O)CCCCCCCCCBr'},
    {'id': 271948, 'r1': 'NCCCC1(C(F)(F)F)N=N1', 'r2': 'NCc1cncc(Cl)c1'},
    {'id': 1458, 'r1': 'Cc1cc(C(=O)O)cc(O)n1', 'r2': 'NC(=O)c1cnc(Cl)cn1'},
    {'id': 'All', 'r1': 'Cc1cc(C(=O)O)cc(O)n1', 'r2': 'NC(=O)c1cnc(Cl)cn1'}
]
reaction_dict = {i['id']:i for i in reaction_data}

def get_reaction_inputs(plugin_record, reaction_id):
    reaction_inputs = reaction_dict[reaction_id]
    r1, r2 = reaction_inputs['r1'], reaction_inputs['r2']
    request_data = {
        'request_id' : None,
        'parents' : [
            {'assembly_index' : 0, 'id' : 1, 'external_id' : '1', 'item' : r1},
            {'assembly_index' : 1, 'id' : 2, 'external_id' : '2', 'item' : r2}
        ]
    }
    request_data['request_id'] = get_request_id(plugin_record)
    return request_data 

def reaction_assembly_backend_executor(backend_client, reaction_id):
    filter_record = fetch_plugins_by_filter(backend_client, 
                                            plugin_type='assembly',
                                            plugin_class='internal_rdkit',
                                            group_key='rdkit_plugin',
                                            name_pattern=f"Enamine Reaction {reaction_id}")[0]
    request_data = get_reaction_inputs(filter_record, reaction_id)
    response = backend_client.post(f"/api/v1/execute/{filter_record['id']}", json=request_data)
    response.raise_for_status()
    result_id = response.json()['result_id']

    result = poll_backend(backend_client, result_id, timeout=20)
    assert 'result_id' not in result 
    assert result['valid']
    assert result['response_data']['valid']
    assert len(result['response_data']['result'])>0

def reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, reaction_id):
    filter_record = fetch_plugins_by_filter(backend_client, 
                                            plugin_type='assembly',
                                            plugin_class='internal_rdkit',
                                            group_key='rdkit_plugin',
                                            name_pattern=f"Enamine Reaction {reaction_id}")[0]
    request_data = get_reaction_inputs(filter_record, reaction_id)
    print(request_data)
    response_data = publish_and_poll(redis_connection, rabbitmq_connection, 
                                     request_data['request_id'], request_data)
    print(response_data)
    assert response_data['valid'] == True
    assert response_data['response_data']['valid'] == True
    assert len(response_data['response_data']['result'])>0

def test_enamine_assembly_created(backend_client):
    plugins = fetch_plugins_by_filter(backend_client, 
                                      plugin_type='assembly',
                                      plugin_class='internal_rdkit',
                                      group_key='rdkit_plugin',
                                      name_pattern=f"%Enamine Reaction%")
    assert len(plugins)==13, "Incorrect number of Enamine assembly plugins"

def test_backend_enamine_reaction_22(backend_client):
    reaction_assembly_backend_executor(backend_client, 22)

def test_backend_enamine_reaction_11(backend_client):
    reaction_assembly_backend_executor(backend_client, 11)

def test_backend_enamine_reaction_527(backend_client):
    reaction_assembly_backend_executor(backend_client, 527)

def test_backend_enamine_reaction_240690(backend_client):
    reaction_assembly_backend_executor(backend_client, 240690)

def test_backend_enamine_reaction_2430(backend_client):
    reaction_assembly_backend_executor(backend_client, 2430)

def test_backend_enamine_reaction_2708(backend_client):
    reaction_assembly_backend_executor(backend_client, 2708)

def test_backend_enamine_reaction_2230(backend_client):
    reaction_assembly_backend_executor(backend_client, 2230)

def test_backend_enamine_reaction_2718(backend_client):
    reaction_assembly_backend_executor(backend_client, 2718)

def test_backend_enamine_reaction_40(backend_client):
    reaction_assembly_backend_executor(backend_client, 40)

def test_backend_enamine_reaction_27(backend_client):
    reaction_assembly_backend_executor(backend_client, 27)

def test_backend_enamine_reaction_271948(backend_client):
    reaction_assembly_backend_executor(backend_client, 271948)

def test_backend_enamine_reaction_1458(backend_client):
    reaction_assembly_backend_executor(backend_client, 1458)

def test_backend_enamine_reaction_all(backend_client):
    reaction_assembly_backend_executor(backend_client, 'All')

def test_consumer_enamine_reaction_22(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 22)

def test_consumer_enamine_reaction_11(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 11)

def test_consumer_enamine_reaction_527(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 527)

def test_consumer_enamine_reaction_240690(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 240690)

def test_consumer_enamine_reaction_2430(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 2430)

def test_consumer_enamine_reaction_2708(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 2708)

def test_consumer_enamine_reaction_2230(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 2230)

def test_consumer_enamine_reaction_2718(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 2718)

def test_consumer_enamine_reaction_40(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 40)

def test_consumer_enamine_reaction_27(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 27)

def test_consumer_enamine_reaction_271948(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 271948)

def test_consumer_enamine_reaction_1458(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 1458)

def test_consumer_enamine_reaction_all(backend_client, redis_connection, rabbitmq_connection):
    reaction_assembly_consumer(backend_client, redis_connection, rabbitmq_connection, 'All')


