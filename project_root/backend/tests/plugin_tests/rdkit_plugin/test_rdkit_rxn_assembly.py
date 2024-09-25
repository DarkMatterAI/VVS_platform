import pytest
import itertools 

api_str = '/api/v1/rdkit_plugins'
plugin_api_str = '/api/v1/plugins'

def default_json(config, num_parents=2):
    default = {
                "name": "Test Assembly",
                "plugin_class": "internal_rdkit",
                "type": "assembly",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "num_parents" : num_parents,
                "config" : config
            }
    return default 

def get_blank_config():
    return {'single_stage_reactions' : [], 'multi_stage_reactions' : []}

async def post_and_validate(data, client, status_code=200):
    response = await client.post(f"{api_str}/", json=data)

    assert response.status_code == status_code

    if status_code==200:
        data = response.json()
        assert data["type"] == "assembly"
        return data 

@pytest.fixture(scope="function")
async def rdkit_test_smarts_assembly(client):
    async def _create_assembly(single_stage=True, multi_stage=True):
        ss = [{'smarts' : 'a.b.c>>d', 'requires_hs' : True}] if single_stage else []
        ms = [
            {'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : True}]},
            {'step' : 1, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : True}]},
        ] if multi_stage else []
        
        config = {'single_stage_reactions' : ss, 'multi_stage_reactions' : ms}
        data = default_json(config, num_parents=3)

        response = await client.post(f"{api_str}/", json=data)
        assert response.status_code == 200
        return response.json()

    return _create_assembly

@pytest.mark.asyncio
async def test_create_rdkit_smarts_assembly(client):
    config = {
        'single_stage_reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}],
        'multi_stage_reactions' : []
    }
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client)
    assert response["config"] == config


@pytest.mark.asyncio
async def test_create_rdkit_smarts_sequence_1_assembly(client):
    config = {
        'single_stage_reactions' : [],
        'multi_stage_reactions' : [{'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]}]
    }
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client)
    assert response["config"] == config

@pytest.mark.asyncio
async def test_create_rdkit_smarts_sequence_2_assembly(client):
    config = {
        'single_stage_reactions' : [],
        'multi_stage_reactions' : [{'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]},
                                   {'step' : 1, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]}]
    }
    data = default_json(config, num_parents=3)
    response = await post_and_validate(data, client)
    assert response["config"] == config

@pytest.mark.asyncio
async def test_invalid_smarts_fails(client):
    invalid_smarts = [
        'a', # missing >>
        '>>a', # missing reactants
        'a>>b', # only 1 reactant
        'a>>', # missing products
        'a>>b.c', # multiple products 
    ]

    for smart in invalid_smarts:
        # for key in ['single_stage_reactions', 'multi_stage_reactions']:
        #     config = get_blank_config()
        ss = {'single_stage_reactions' : [{'smarts' : smart, 'requires_hs' : False}]}
        ms = {'multi_stage_reactions' : [{'step' : 0, 'reactions' : [{'smarts' : smart, 'requires_hs' : False}]}]}

        for config_update in [ss, ms]:
            config = get_blank_config()
            config.update(config_update)
            assert len(config['single_stage_reactions']) + len(config['multi_stage_reactions']) != 0
            data = default_json(config, num_parents=2)
            response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_single_stage_reactants_match(client):
    config = get_blank_config()
    config['single_stage_reactions'] = [{'smarts' : 'a.b>>c', 'requires_hs' : False},
                                         {'smarts' : 'a.b.c>>d', 'requires_hs' : False}]
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_single_stage_parents_match(client):
    config = get_blank_config()
    config['single_stage_reactions'] = [{'smarts' : 'a.b.c>>d', 'requires_hs' : False},
                                         {'smarts' : 'a.b.c>>d', 'requires_hs' : False}]
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_multi_stage_reactants_count(client):
    config = get_blank_config()
    config['multi_stage_reactions'] = [{'step' : 0, 'reactions' : [{'smarts' : 'a.b.c>>d', 'requires_hs' : False}]}]
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_multi_stage_duplicate_step(client):
    config = get_blank_config()
    config['multi_stage_reactions'] = [{'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]},
                                       {'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]}]
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_multi_stage_insufficient_steps(client):
    config = get_blank_config()
    config['multi_stage_reactions'] = [{'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]}]
    data = default_json(config, num_parents=3)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_multi_stage_too_many_steps(client):
    config = get_blank_config()
    config['multi_stage_reactions'] = [{'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]},
                                       {'step' : 0, 'reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}]}]
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_no_reactions(client):
    config = get_blank_config()
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_read_rdkit_smarts_assembly(client, rdkit_test_smarts_assembly):
    record = await rdkit_test_smarts_assembly()
    response = await client.get(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 200
    response = response.json()
    assert response == record

@pytest.mark.asyncio
async def test_update_rdkit_smarts_assembly(client, rdkit_test_smarts_assembly):
    record = await rdkit_test_smarts_assembly(multi_stage=False)
    new_config = {'single_stage_reactions' : [{'smarts' : 'a.b>>c', 'requires_hs' : False}],
                     'multi_stage_reactions' : []}
    update_data = {"num_parents" : 2, "config" : new_config}
    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 200
    response = response.json()
    assert response['config'] == new_config 


@pytest.mark.asyncio
async def test_invalid_update_rdkit_smarts_assembly(client, rdkit_test_smarts_assembly):
    record = await rdkit_test_smarts_assembly()
    new_config = {'single_stage_reactions' : [{'smarts' : 'a.b.c>>d', 'requires_hs' : False}],
                     'multi_stage_reactions' : []}
    update_data = {"num_parents" : 2, "config" : new_config}
    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_delete_rdkit_smarts_assembly(client, rdkit_test_smarts_assembly):
    record = await rdkit_test_smarts_assembly()
    response = await client.delete(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 200

    response = await client.delete(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 404


