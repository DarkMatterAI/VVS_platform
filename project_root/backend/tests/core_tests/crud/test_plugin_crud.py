import pytest

api_str = '/api/v1/plugins'

@pytest.mark.asyncio
async def test_main(client):
    response = await client.get('/')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_create_api_requires_url(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_requires_group_key(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Embedding",
            "plugin_class": "generic",
            "type": "embedding",
            "execution_type": "queue",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "vector_length": 128,
            "distance_metric": "Cosine"
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_requires_timeout(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_requires_concurrency(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_retries": 1,
            "endpoint_url": "http://test.com/filter",
        }
    )
    assert response.status_code == 422 

@pytest.mark.asyncio
async def test_positive_batch_size(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_retries": 1,
            "batch_size": -1,
            "endpoint_url": "http://test.com/filter",
        }
    )
    assert response.status_code == 422 

@pytest.mark.asyncio
async def test_create_requires_retries(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "max_concurrency": 10,
            "timeout": 30,
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_embedding_plugin(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Embedding",
            "plugin_class": "generic",
            "type": "embedding",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "vector_length": 128,
            "distance_metric": "Cosine"
        }
    )
    assert response.status_code == 200
    data = response.json()
    print(data)
    assert data["type"] == "embedding"
    assert data["vector_length"] == 128
    assert data["distance_metric"] == "Cosine"

@pytest.mark.asyncio
async def test_invalid_plugin_type_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Embedding",
            "plugin_class": "generic",
            "type": "awresty",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "vector_length": 128,
            "distance_metric": "Cosine"
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_invalid_plugin_class_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Embedding",
            "plugin_class": "awresty",
            "type": "embedding",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "vector_length": 128,
            "distance_metric": "Cosine"
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_data_source_plugin(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "data_source"
    assert data["embedding_ids"] == [embedding.id]

@pytest.mark.asyncio
async def test_create_data_source_plugin_multiple_embeddings(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()

    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding1.id, embedding2.id]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "data_source"
    assert data["embedding_ids"] == [embedding1.id, embedding2.id]

@pytest.mark.asyncio
async def test_create_data_source_plugin_fails_without_embedding(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": []
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_filter_plugin(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "endpoint_url": "http://test.com/filter",
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["endpoint_url"] == "http://test.com/filter"
    assert data['embedding_ids'] == None

@pytest.mark.asyncio
async def test_create_filter_plugin_with_embedding(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "endpoint_url": "http://test.com/filter",
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["embedding_ids"] == [embedding.id]

@pytest.mark.asyncio
async def test_create_filter_plugin_with_embeddings(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "endpoint_url": "http://test.com/filter",
            "embedding_ids": [embedding1.id, embedding2.id]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "filter"
    assert data["embedding_ids"] == [embedding1.id, embedding2.id]

@pytest.mark.asyncio
async def test_create_score_plugin(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Score",
            "plugin_class": "generic",
            "type": "score",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "embedding_ids": []
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "score"
    assert data["embedding_ids"] == None

@pytest.mark.asyncio
async def test_create_score_plugin_with_embedding(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Score",
            "plugin_class": "generic",
            "type": "score",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "score"
    assert data["embedding_ids"] == [embedding.id]

@pytest.mark.asyncio
async def test_create_score_plugin_with_embeddings(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Score",
            "plugin_class": "generic",
            "type": "score",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding1.id, embedding2.id]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "score"
    assert data["embedding_ids"] == [embedding1.id, embedding2.id]

@pytest.mark.asyncio
async def test_create_mapper_plugin(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    embedding3 = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Mapper",
            "plugin_class": "generic",
            "type": "mapper",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries" : 1,
            "input_embedding_id": embedding1.id,
            "output_order": [{"index": 0, "embedding_id": embedding2.id},
                             {"index": 1, "embedding_id": embedding2.id}]
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "mapper"
    assert data["input_embedding_id"] == embedding1.id
    assert data["output_order"] == [{"index": 0, "embedding_id": embedding2.id},
                                         {"index": 1, "embedding_id": embedding2.id}]

@pytest.mark.asyncio
async def test_create_mapper_plugin_fails_without_input_embedding(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Mapper",
            "plugin_class": "generic",
            "type": "mapper",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "output_order": [{"index": 0, "embedding_id": embedding.id},
                             {"index": 1, "embedding_id": embedding.id}]
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_mapper_plugin_duplicate_output_index_fails(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Mapper",
            "plugin_class": "generic",
            "type": "mapper",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "input_embedding_id": embedding.id,
            "output_order": [{"index": 0, "embedding_id": embedding.id},
                             {"index": 0, "embedding_id": embedding.id}]
        }
    )
    assert response.status_code == 422, response.text

@pytest.mark.asyncio
async def test_create_mapper_plugin_fails_with_insufficient_output_embeddings(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Mapper",
            "plugin_class": "generic",
            "type": "mapper",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "input_embedding_id": embedding1.id,
            "output_order": [{"index": 0, "embedding_id": embedding2.id}]
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_assembly_plugin(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Assembly",
            "plugin_class": "generic",
            "type": "assembly",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "num_parents": 3
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "assembly"
    assert data["num_parents"] == 3

@pytest.mark.asyncio
async def test_create_assembly_plugin_fails_with_insufficient_parents(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Assembly",
            "plugin_class": "generic",
            "type": "assembly",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "num_parents": 1
        }
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_embedding_plugin_with_duplicate_embeddings(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding.id, embedding.id]
        }
    )
    assert response.status_code == 422
    assert "Duplicate embedding IDs are not allowed" in response.json()["detail"]

@pytest.mark.asyncio
async def test_create_data_source_with_invalid_embedding_id(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [999]  # Assuming 999 is an invalid ID
        }
    )
    assert response.status_code == 422
    assert "Invalid embedding IDs" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_plugin(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.get(f"{api_str}/{embedding.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == embedding.id
    assert data["type"] == "embedding"

@pytest.mark.asyncio
async def test_update_embedding_plugin(client, create_test_embedding):
    embedding = await create_test_embedding()
    update_data = {
        "name": "Updated Embedding",
        "vector_length": 256,
        "distance_metric": "Euclid"
    }
    response = await client.put(f"{api_str}/{embedding.id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Embedding"
    assert data["vector_length"] == 256
    assert data["distance_metric"] == "Euclid"

@pytest.mark.asyncio
async def test_update_data_source_plugin(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding1.id]
        }
    )
    assert response.status_code == 200
    data_source_id = response.json()["id"]
    
    update_data = {
        "name": "Updated Data Source",
        "embedding_ids": [embedding1.id, embedding2.id]
    }
    response = await client.put(f"{api_str}/{data_source_id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Data Source"
    assert set(data["embedding_ids"]) == {embedding1.id, embedding2.id}

@pytest.mark.asyncio
async def test_update_filter_plugin(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Filter",
            "plugin_class": "generic",
            "type": "filter",
            "execution_type": "api",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "endpoint_url": "http://test.com/filter",
            "embedding_ids": []
        }
    )
    assert response.status_code == 200
    filter_id = response.json()["id"]

    update_data = {
        "name": "Updated Filter",
        "embedding_ids": [embedding.id]
    }
    response = await client.put(f"{api_str}/{filter_id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Filter"
    assert data["embedding_ids"] == [embedding.id]

@pytest.mark.asyncio
async def test_update_score_plugin(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Score",
            "plugin_class": "generic",
            "type": "score",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "embedding_ids": []
        }
    )
    assert response.status_code == 200
    score_id = response.json()["id"]

    update_data = {
        "name": "Updated Score",
        "embedding_ids": [embedding.id]
    }
    response = await client.put(f"{api_str}/{score_id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Score"
    assert data["embedding_ids"] == [embedding.id]

@pytest.mark.asyncio
async def test_update_mapper_plugin(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    embedding3 = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Mapper",
            "plugin_class": "generic",
            "type": "mapper",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "input_embedding_id": embedding1.id,
            "output_order": [{"index": 0, "embedding_id": embedding2.id},
                             {"index": 1, "embedding_id": embedding3.id}]
        }
    )
    assert response.status_code == 200
    mapper_id = response.json()["id"]

    update_data = {
        "name": "Updated Mapper",
        "input_embedding_id": embedding2.id,
        "output_order": [{"index": 0, "embedding_id": embedding2.id},
                         {"index": 1, "embedding_id": embedding2.id}]
    }
    response = await client.put(f"{api_str}/{mapper_id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Mapper"
    assert data["input_embedding_id"] == embedding2.id
    assert data["output_order"] == [{"index": 0, "embedding_id": embedding2.id},
                                                 {"index": 1, "embedding_id": embedding2.id}]

@pytest.mark.asyncio
async def test_update_assembly_plugin(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Assembly",
            "plugin_class": "generic",
            "type": "assembly",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "num_parents": 3
        }
    )
    assert response.status_code == 200
    assembly_id = response.json()["id"]

    update_data = {
        "name": "Updated Assembly",
        "num_parents": 4
    }
    response = await client.put(f"{api_str}/{assembly_id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Assembly"
    assert data["num_parents"] == 4

@pytest.mark.asyncio
async def test_update_data_source_with_zero_embeddings_fails(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 200
    data_source_id = response.json()["id"]

    update_data = {
        "embedding_ids": []
    }
    response = await client.put(f"{api_str}/{data_source_id}", json=update_data)
    assert response.status_code == 422
    assert "validation error for DataSourcePluginInDB" in response.json()["detail"]

@pytest.mark.asyncio
async def test_update_assembly_with_less_than_two_parents_fails(client):
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Assembly",
            "plugin_class": "generic",
            "type": "assembly",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 30,
            "max_concurrency": 5,
            "max_retries": 1,
            "num_parents": 3
        }
    )
    assert response.status_code == 200
    assembly_id = response.json()["id"]

    update_data = {
        "num_parents": 1
    }
    response = await client.put(f"{api_str}/{assembly_id}", json=update_data)
    assert response.status_code == 422
    assert "validation error for AssemblyPluginInDB" in response.json()["detail"]

@pytest.mark.asyncio
async def test_update_mapper_with_less_than_two_output_embeddings_fails(client, create_test_embedding):
    embedding1 = await create_test_embedding()
    embedding2 = await create_test_embedding()
    embedding3 = await create_test_embedding()
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Mapper",
            "plugin_class": "generic",
            "type": "mapper",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "input_embedding_id": embedding1.id,
            "output_order": [{"index": 0, "embedding_id": embedding2.id},
                             {"index": 1, "embedding_id": embedding3.id}]
        }
    )
    assert response.status_code == 200
    mapper_id = response.json()["id"]

    update_data = {
        "output_order": [{"index": 0, "embedding_id": embedding2.id}]
    }

    response = await client.put(f"{api_str}/{mapper_id}", json=update_data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_delete_plugin(client, create_test_embedding):
    embedding = await create_test_embedding()
    response = await client.delete(f"{api_str}/{embedding.id}")
    assert response.status_code == 200

    response = await client.get(f"{api_str}/{embedding.id}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_delete_linked_embedding_plugin_fails(client, create_test_embedding):
    embedding = await create_test_embedding()
    
    response = await client.post(
        f"{api_str}/",
        json={
            "name": "Test Data Source",
            "plugin_class": "generic",
            "type": "data_source",
            "execution_type": "queue",
            "group_key": "test",
            "timeout": 60,
            "max_concurrency": 10,
            "max_retries": 1,
            "embedding_ids": [embedding.id]
        }
    )
    assert response.status_code == 200

    response = await client.delete(f"{api_str}/{embedding.id}")
    assert response.status_code == 400
    assert "Cannot delete this embedding plugin" in response.json()["detail"]

@pytest.mark.asyncio
async def test_scroll_plugins(client, create_test_embedding):
    num_plugins = 15
    for i in range(num_plugins):
        await create_test_embedding(name=f"Test Embedding {i}")

    response = await client.get(f"{api_str}/?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5

    response = await client.get(f"{api_str}/?skip=10&limit=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5
