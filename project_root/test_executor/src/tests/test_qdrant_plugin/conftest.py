import os 
from qdrant_client import QdrantClient 
import pytest
import itertools
import numpy as np 
import string 

api_str = '/api/v1/qdrant_plugins'
plugin_api_str = '/api/v1/plugins'

_embedding_counter = itertools.count(1)
_data_counter = itertools.count(1)
random_qdrant_key = ''.join(np.random.choice([i for i in string.ascii_lowercase], 8))

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
                "name": f"Test Qdrant Integration Embedding {next(_embedding_counter)} {random_qdrant_key}",
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
                "name": f"Test Qdrant Integration Data Source {_data_counter} {random_qdrant_key}",
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
        assert response.status_code == 200, response.text
        return response.json()

    return _test_data_source
