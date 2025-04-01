import pytest
import itertools 

api_str = '/api/v1/rdkit_plugins'
plugin_api_str = '/api/v1/plugins'

def default_json(config):
    default = {
            "name": "Test Score",
            "plugin_class": "internal_rdkit",
            "type": "score",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : config
        }
    return default 

async def post_and_validate(data, client, status_code=200):
    response = await client.post(f"{api_str}/", json=data)

    assert response.status_code == status_code

    if status_code==200:
        data = response.json()
        assert data["type"] == "score"
        assert data["group_key"] == "rdkit_plugin"
        return data 

@pytest.fixture(scope="function")
async def rdkit_test_score(client):
    async def _create_score():
        config = {"property_weights": [{"property_name": "QED", "weight": 1.0}]}
        data = default_json(config)
        data['name'] = f"Test RDKit Filter Backend {next(itertools.count(1))}"
        response = await post_and_validate(data, client)
        return response 

    return _create_score

@pytest.mark.asyncio
async def test_create_rdkit_score(client):
    config = {"property_weights": [{"property_name": "QED", "weight": 1.0}]}
    data = default_json(config)
    result = await post_and_validate(data, client)
    assert result["config"]["property_weights"][0]["property_name"] == "QED"

@pytest.mark.asyncio
async def test_rdkit_score_invalid_property_fails(client):
    config = {"property_weights" : [{"property_name" : "TPSA", "weight" : 10}, 
                                    {"property_name" : "asfbsdg", "weight" : 10}]}
    data = default_json(config)
    result = await post_and_validate(data, client, 422)

@pytest.mark.asyncio
async def test_read_rdkit_score(client, rdkit_test_score):
    record = await rdkit_test_score()
    response = await client.get(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 200
    response = response.json()
    assert response == record

@pytest.mark.asyncio
async def test_update_rdkit_score(client, rdkit_test_score):
    record = await rdkit_test_score()
    update_data = {"config" : {"property_weights" : [{"property_name": "TPSA", "weight": 10}]}}
    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 200
    response = response.json()
    assert response['config'] == update_data['config']

@pytest.mark.asyncio
async def test_invalid_update_rdkit_score(client, rdkit_test_score):
    record = await rdkit_test_score()
    update_data = {"config" : {}}
    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_delete_rdkit_score(client, rdkit_test_score):
    record = await rdkit_test_score()
    response = await client.delete(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 200

    response = await client.delete(f"{plugin_api_str}/{record['id']}")
    assert response.status_code == 404



