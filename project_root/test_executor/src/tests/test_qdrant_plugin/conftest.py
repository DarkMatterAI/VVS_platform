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
def test_embedding(backend_client, plugin_cleanup):
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
        response.raise_for_status()
        response = response.json()
        plugin_cleanup(response)
        return response 
    
    return _test_embedding 


@pytest.fixture(scope="function")
def test_data_source(backend_client, test_embedding, plugin_cleanup):
    def _test_data_source(n_embeddings):
        embedding_records = [test_embedding() for i in range(n_embeddings)]
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
        response.raise_for_status()
        response = response.json()
        plugin_cleanup(response)
        return response, embedding_records 

    return  _test_data_source


