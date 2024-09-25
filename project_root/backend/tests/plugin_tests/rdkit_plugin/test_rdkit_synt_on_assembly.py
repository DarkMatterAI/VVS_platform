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
    return {'synt_on_reaction_stages' : []}

async def post_and_validate(data, client, status_code=200):
    response = await client.post(f"{api_str}/", json=data)

    assert response.status_code == status_code

    if status_code==200:
        data = response.json()
        assert data["type"] == "assembly"
        return data 

@pytest.fixture(scope="function")
async def rdkit_test_synton_assembly(client):
    async def _create_assembly(num_parents=2):

        config = {'synt_on_reaction_stages' : [{'step': i, 'reactions' : ["O-acylation"]}
                                               for i in range(num_parents-1)]}
        
        data = default_json(config, num_parents=num_parents)
        response = await client.post(f"{api_str}/", json=data)
        assert response.status_code == 200
        return response.json()
    return _create_assembly

@pytest.mark.asyncio
async def test_create_synton_assembly(client):
    num_parents = 2
    config = {'synt_on_reaction_stages' : [{'step': i, 'reactions' : ["O-acylation"]}
                                               for i in range(num_parents-1)]}
    data = default_json(config, num_parents=num_parents)
    response = await post_and_validate(data, client)
    assert response["config"] == config

@pytest.mark.asyncio
async def test_create_synton_assembly_multistep(client):
    num_parents = 5
    config = {'synt_on_reaction_stages' : [{'step': i, 'reactions' : ["O-acylation"]}
                                               for i in range(num_parents-1)]}
    data = default_json(config, num_parents=num_parents)
    response = await post_and_validate(data, client)
    assert response["config"] == config

@pytest.mark.asyncio
async def test_synton_invalid_name_fails(client):
    config = {'synt_on_reaction_stages' : [{'step': 0, 'reactions' : ["adfabgf"]}]}
    data = default_json(config, num_parents=2)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_create_synton_assembly_duplucate_steps(client):
    num_parents = 3
    config = {'synt_on_reaction_stages' : [{'step': 0, 'reactions' : ["O-acylation"]}
                                               for i in range(num_parents-1)]}
    data = default_json(config, num_parents=num_parents)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_create_synton_assembly_insufficient_steps(client):
    num_parents = 5
    config = {'synt_on_reaction_stages' : [{'step': 0, 'reactions' : ["O-acylation"]}]}
    data = default_json(config, num_parents=num_parents)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_create_synton_assembly_excess_steps(client):
    num_parents = 2
    config = {'synt_on_reaction_stages' : [{'step': i, 'reactions' : ["O-acylation"]}
                                           for i in range(5)]}
    data = default_json(config, num_parents=num_parents)
    response = await post_and_validate(data, client, status_code=422)

@pytest.mark.asyncio
async def test_create_synton_assembly_no_reactions(client):
    num_parents = 2
    config = {'synt_on_reaction_stages' : []}
    data = default_json(config, num_parents=num_parents)
    response = await post_and_validate(data, client, status_code=422)


@pytest.mark.asyncio
async def test_cread_synton_assembly(client, rdkit_test_synton_assembly):
    record = await rdkit_test_synton_assembly()
    response = await client.get(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 200
    response = response.json()
    assert response == record

@pytest.mark.asyncio
async def test_update_synton_assembly(client, rdkit_test_synton_assembly):
    num_parents = 3
    record = await rdkit_test_synton_assembly(num_parents=num_parents)
    new_config = {'synt_on_reaction_stages' : [{'step': i, 'reactions' : ["S-alkylation_arylation"]}
                                               for i in range(num_parents-1)]}
    update_data = {"num_parents" : num_parents, "config" : new_config}
    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 200
    response = response.json()
    assert response['config'] == new_config 

@pytest.mark.asyncio
async def test_invalid_update_synton_assembly(client, rdkit_test_synton_assembly):
    num_parents = 3
    record = await rdkit_test_synton_assembly(num_parents=num_parents)
    new_config = {'synt_on_reaction_stages' : [{'step': 0, 'reactions' : ["S-alkylation_arylation"]}]}
    update_data = {"num_parents" : num_parents, "config" : new_config}
    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_delete_rdkit_smarts_assembly(client, rdkit_test_synton_assembly):
    record = await rdkit_test_synton_assembly()
    response = await client.delete(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 200

    response = await client.delete(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 404


