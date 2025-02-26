import os 
from qdrant_client import QdrantClient, models 
import pytest
import itertools
import numpy as np 
from tests.utils import type_to_request_func, delete_plugin
from tests.test_helpers import execute_plugin_helper
from vvs_database.schemas import PluginType

api_str = '/api/v1/qdrant_plugins'
plugin_api_str = '/api/v1/plugins'

@pytest.fixture(scope="function")
def qdrant_client():
    client = QdrantClient(location='qdrant', 
                          port=int(os.environ.get('QDRANT__SERVICE__HTTP_PORT', 6333)), 
                          grpc_port=int(os.environ.get('QDRANT__SERVICE__GRPC_PORT', 6334)),
                          prefer_grpc=False,
                          timeout=60
                         )
    yield client 
    client.close()

@pytest.fixture(scope="function")
def test_embedding(backend_client):
    def _test_embedding():
        response = backend_client.post(
            f"{plugin_api_str}/",
            json={
                "name": f"Test Qdrant Integration Embedding {next(itertools.count(1))}",
                "plugin_class": "generic",
                "type": "embedding",
                "execution_type": "queue",
                "group_key": "fake_queue",
                "timeout": 30,
                "max_concurrency": 5,
                "max_retries": 1,
                "vector_length": 32,
                "distance_metric": "Euclid",
                "config": {}
            }
        )
        assert response.status_code == 200
        return response.json()

    return _test_embedding

@pytest.fixture(scope="function")
def test_data_source(backend_client):
    def _test_data_source(embedding_records):
        response = backend_client.post(
            f"{api_str}/",
            json={
                "name": f"Test Qdrant Integration Data Source {next(itertools.count(1))}",
                "qdrant_config": {
                    "vectors_config": [
                        {
                            "embedding_id": embedding_record['id'],
                            "on_disk": False,
                            "datatype": "float16"
                        }
                        for embedding_record in embedding_records
                    ]
                }
            }
        )
        assert response.status_code == 200
        return response.json()

    return _test_data_source

def test_qdrant_ping(qdrant_client):
    collections = qdrant_client.get_collections()
    print(collections)
    assert True 

def test_qdrant_crud_two_embeddings(backend_client, qdrant_client, test_embedding):
    _ = qdrant_client.get_collections()
    embedding_record1 = test_embedding()
    embedding_record2 = test_embedding()

    create_data = {
        "name": "test_data_source",
        "qdrant_config": {
            "vectors_config": [
                {
                    "embedding_id": embedding_record1['id'],
                    "on_disk": False,
                    "datatype": "float16"
                },
                {
                    "embedding_id": embedding_record2['id'],
                    "on_disk": False,
                    "datatype": "float16"
                }
            ]
        }
    }

    response = backend_client.post(f"{api_str}/", json=create_data)
    assert response.status_code == 200
    data_record = response.json()

    data_source_id = data_record['id']
    collection_name = f"data_source_{data_source_id}"
    collection_info = qdrant_client.get_collection(collection_name)

    delete_plugin(data_record, backend_client, plugin_api_str)
    delete_plugin(embedding_record1, backend_client, plugin_api_str)
    delete_plugin(embedding_record2, backend_client, plugin_api_str)

    collections = qdrant_client.get_collections()
    collection_names = [i['name'] for i in collections.model_dump()['collections']]
    assert collection_name not in collection_names 

def test_qdrant_crud(backend_client, qdrant_client, test_embedding):
    _ = qdrant_client.get_collections()
    embedding_record = test_embedding()

    create_data = {
        "name": "test_data_source",
        "qdrant_config": {
            "vectors_config": [
            {
                "embedding_id": embedding_record['id'],
                "on_disk": False,
                "datatype": "float16"
            }
            ]
        }
    }

    response = backend_client.post(f"{api_str}/", json=create_data)
    assert response.status_code == 200
    data_record = response.json()

    data_source_id = data_record['id']
    collection_name = f"data_source_{data_source_id}"
    collection_info = qdrant_client.get_collection(collection_name)

    delete_plugin(data_record, backend_client, plugin_api_str)
    delete_plugin(embedding_record, backend_client, plugin_api_str)

    collections = qdrant_client.get_collections()
    collection_names = [i['name'] for i in collections.model_dump()['collections']]
    assert collection_name not in collection_names 

def test_qdrant_execute(backend_client, qdrant_client, test_embedding, test_data_source):
    _ = qdrant_client.get_collections()
    embedding_record = test_embedding()
    data_record = test_data_source([embedding_record])

    collection_name = f"data_source_{data_record['id']}"
    embedding_name = f"embedding_{embedding_record['id']}"

    n_points = 32
    qdrant_client.upsert(
        collection_name=collection_name,
        points=models.Batch(
            ids=[i+1 for i in range(n_points)],
            payloads=[{'item': f"item_{i}", 'external_id': i} for i in range(n_points)],
            vectors={embedding_name: np.random.rand(n_points, 32).tolist()}
        )
    )
    
    # Use helper for plugin execution
    plugins = [data_record]
    request_func = type_to_request_func[PluginType.DATA_SOURCE]
    data_request = request_func(data_record, embedding_index=embedding_record['id'])
    result = execute_plugin_helper(backend_client, plugins, PluginType.DATA_SOURCE, 
                                   timeout=20, custom_request=data_request)
    assert result is not None

    delete_plugin(data_record, backend_client, plugin_api_str)
    delete_plugin(embedding_record, backend_client, plugin_api_str)

def test_qdrant_wrong_embedding(backend_client, qdrant_client, test_embedding, test_data_source):
    _ = qdrant_client.get_collections()
    embedding_record = test_embedding()
    data_record = test_data_source([embedding_record])

    collection_name = f"data_source_{data_record['id']}"
    embedding_name = f"embedding_{embedding_record['id']}"

    n_points = 32
    qdrant_client.upsert(
        collection_name=collection_name,
        points=models.Batch(
            ids=[i+1 for i in range(n_points)],
            payloads=[{'item': f"item_{i}", 'external_id': i} for i in range(n_points)],
            vectors={embedding_name: np.random.rand(n_points, 32).tolist()}
        )
    )

    alt_embedding_record = test_embedding()
    
    # Use helper for plugin execution
    plugins = [data_record]
    request_func = type_to_request_func[PluginType.DATA_SOURCE]
    data_request = request_func(data_record, embedding_index=alt_embedding_record['id'])

    endpoint = f"/api/v1/execute/{data_record['id']}"
    response = backend_client.post(endpoint, json=data_request)
    assert response.status_code == 500

    delete_plugin(data_record, backend_client, plugin_api_str)
    delete_plugin(embedding_record, backend_client, plugin_api_str)
    delete_plugin(alt_embedding_record, backend_client, plugin_api_str)


def test_qdrant_crud_two_embeddings_execute(backend_client, qdrant_client, test_embedding, test_data_source):
    _ = qdrant_client.get_collections()
    embedding_record1 = test_embedding()
    embedding_record2 = test_embedding()

    data_record = test_data_source([embedding_record1, embedding_record2])

    collection_name = f"data_source_{data_record['id']}"
    embedding_name1 = f"embedding_{embedding_record1['id']}"
    embedding_name2 = f"embedding_{embedding_record2['id']}"

    n_points = 32
    qdrant_client.upsert(
        collection_name=collection_name,
        points=models.Batch(
            ids=[i+1 for i in range(n_points)],
            payloads=[{'item': f"item_{i}", 'external_id': i} for i in range(n_points)],
            vectors={
                embedding_name1: np.random.rand(n_points, 32).tolist(),
                embedding_name2: np.random.rand(n_points, 32).tolist()
            }
        )
    )
    
    request_func = type_to_request_func[PluginType.DATA_SOURCE]
    
    # Using direct API calls because we need to test each embedding specifically
    data_request_1 = request_func(data_record, embedding_index=embedding_record1['id'])
    response_1 = backend_client.post(
        f"/api/v1/execute/{data_record['id']}", 
        json=data_request_1, 
        timeout=20
    )
    assert response_1.status_code == 200    

    data_request_2 = request_func(data_record, embedding_index=embedding_record2['id'])
    response_2 = backend_client.post(
        f"/api/v1/execute/{data_record['id']}", 
        json=data_request_2, 
        timeout=20
    )
    assert response_2.status_code == 200    
    
    delete_plugin(data_record, backend_client, plugin_api_str)
    delete_plugin(embedding_record1, backend_client, plugin_api_str)
    delete_plugin(embedding_record2, backend_client, plugin_api_str)

