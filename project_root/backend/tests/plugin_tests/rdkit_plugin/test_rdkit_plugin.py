import pytest
import itertools 
from app import models

api_str = '/api/v1/rdkit_plugins'

@pytest.fixture(scope="function")
async def rdkit_test_filter(client):
    async def _create_filter():
        response = await client.post(
            f"{api_str}/",
            json={
                "name": f"Test RDKit Filter {next(itertools.count(1))}",
                "type": "filter",
                "execution_type": "queue",
                "group_key": "rdkit_plugin",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "config": {"property_filters": [{"property_name": "TPSA", "min_val": 10}]}
            }
        )
        assert response.status_code == 200
        return response.json()

    return _create_filter

@pytest.mark.asyncio
async def test_create_rdkit_filter_with_property(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "property_filters" : [
                    {"property_name" : "TPSA", "min_val" : 10}
                ]
            }
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["config"]["property_filters"][0]["max_val"] == None

@pytest.mark.asyncio
async def test_create_rdkit_filter_with_catalog(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "catalog_filters" : [
                    {"catalog_name": "PAINS"}
                ]
            }
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["config"]["catalog_filters"][0]["catalog_name"] == "PAINS"

@pytest.mark.asyncio
async def test_create_rdkit_filter_with_smarts(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "smarts_filters" : [
                    {"smarts": "[#6]", "max_val" : 1}
                ]
            }
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["config"]["smarts_filters"][0]["max_val"] == 1

@pytest.mark.asyncio
async def test_rdkit_filter_replace_vals(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "embedding",
            "execution_type": "api",
            "group_key": "other",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "property_filters" : [
                    {"property_name" : "TPSA", "min_val" : 10}
                ]
            }
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["execution_type"] == "queue"
    assert data["group_key"] == "rdkit_plugin"


@pytest.mark.asyncio
async def test_rdkit_filter_duplicate_property_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "property_filters" : [
                    {"property_name" : "TPSA", "min_val" : 10},
                    {"property_name" : "TPSA", "min_val" : 10}
                ]
            }
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_rdkit_filter_invalid_property_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "property_filters" : [
                    {"property_name" : "TPSA", "min_val" : 10},
                    {"property_name" : "asfbsdg", "min_val" : 10}
                ]
            }
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_rdkit_filter_duplicate_catalog_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "catalog_filters" : [
                    {"catalog_name": "PAINS"},
                    {"catalog_name": "PAINS"}
                ]
            }
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_rdkit_filter_invalid_catalog_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "catalog_filters" : [
                    {"catalog_name": "asfbsdb"},
                ]
            }
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_rdkit_filter_duplicate_smarts_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "smarts_filters" : [
                    {"smarts": "[#6]", "max_val" : 1},
                    {"smarts": "[#6]", "max_val" : 1}
                ]
            }
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_rdkit_filter_empty_filter_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
            }
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_rdkit_filter_min_max_stripping(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "type": "filter",
            "execution_type": "queue",
            "group_key": "rdkit_plugin",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "config" : {
                "property_filters" : [
                    {"property_name" : "TPSA", "min_val" : 10},
                    {"property_name" : "Molecular Weight"}
                ],
                "smarts_filters" : [
                    {"smarts": "[#6]", "max_val" : 1},
                    {"smarts": "[#7]", "min_val" : None, "max_val" : None}
                ]
            }
        }
    )
    assert response.status_code == 200
    data = response.json()

    assert len(data["config"]["property_filters"]) == 1
    assert len(data["config"]["smarts_filters"]) == 1

@pytest.mark.asyncio
async def test_read_rdkit_filter(client, rdkit_test_filter):
    record = await rdkit_test_filter()
    response = await client.get(f"{api_str}/{record['id']}")
    assert response.status_code == 200
    response = response.json()
    assert response == record

@pytest.mark.asyncio
async def test_update_rdkit_filter(client, rdkit_test_filter):
    record = await rdkit_test_filter()

    update_data = {
        "config" : {
            "catalog_filters" : [
                {"catalog_name": "PAINS"},
            ]
        }
    }

    response = await client.put(f"{api_str}/{record['id']}", json=update_data)
    assert response.status_code == 200
    response = response.json()
    assert len(response['config']['property_filters']) == 0
    assert len(response['config']['catalog_filters']) == 1
    assert response['config']['catalog_filters'][0]['catalog_name'] == 'PAINS'


@pytest.mark.asyncio
async def test_delete_rdkit_filter(client, rdkit_test_filter):
    record = await rdkit_test_filter()
    response = await client.delete(f"{api_str}/{record['id']}")
    assert response.status_code == 200

    response = await client.delete(f"{api_str}/{record['id']}")
    assert response.status_code == 404

