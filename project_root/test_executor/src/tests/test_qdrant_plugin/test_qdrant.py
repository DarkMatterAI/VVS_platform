import pytest 
from qdrant_client import models 
import pytest
import numpy as np 

from tests.utils.request_data import validate_api_response, generate_request_data
from tests.utils.backend_utils import backend_execute_plugin, backend_delete_plugin
from tests.utils.db_utils import validate_data_source_checkin

api_str = '/api/v1/qdrant_plugins'
plugin_api_str = '/api/v1/plugins'

def test_qdrant_ping(qdrant_client):
    collections = qdrant_client.get_collections()
    print(collections)
    assert True 

def test_qdrant_crud(backend_client, qdrant_client, test_embedding, test_data_source):
    embedding_record = test_embedding()
    data_record = test_data_source([embedding_record])

    data_source_id = data_record['id']
    collection_name = f"data_source_{data_source_id}"
    collection_info = qdrant_client.get_collection(collection_name)
    assert collection_info

    backend_delete_plugin(backend_client, plugin_api_str, data_record)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record)

    collections = qdrant_client.get_collections()
    collection_names = [i['name'] for i in collections.model_dump()['collections']]
    assert collection_name not in collection_names 

def test_qdrant_crud_two_embeddings(backend_client, qdrant_client, test_embedding, test_data_source):
    embedding_record1 = test_embedding()
    embedding_record2 = test_embedding()
    data_record = test_data_source([embedding_record1, embedding_record2])

    data_source_id = data_record['id']
    collection_name = f"data_source_{data_source_id}"
    collection_info = qdrant_client.get_collection(collection_name)
    assert collection_info

    backend_delete_plugin(backend_client, plugin_api_str, data_record)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record1)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record2)

    collections = qdrant_client.get_collections()
    collection_names = [i['name'] for i in collections.model_dump()['collections']]
    assert collection_name not in collection_names 

def qdrant_upsert(qdrant_client, data_record, embedding_records, n_points):
    collection_name = f"data_source_{data_record['id']}"
    embedding_names = [f"embedding_{i['id']}" for i in embedding_records]
    embedding_sizes = [i['vector_length'] for i in embedding_records]

    qdrant_client.upsert(
        collection_name=collection_name,
        points=models.Batch(
            ids=[i+1 for i in range(n_points)],
            payloads=[{'item': f"item_{i}", 'external_id': i} for i in range(n_points)],
            vectors={embedding_names[i]:np.random.rand(n_points, embedding_sizes[i]).tolist()
                     for i in range(len(embedding_records))}
        )
    )

@pytest.mark.asyncio
async def test_qdrant_execute(db_session, backend_client, qdrant_client, test_embedding, test_data_source):
    embedding_record = test_embedding()
    data_record = test_data_source([embedding_record])
    qdrant_upsert(qdrant_client, data_record, [embedding_record], 32)

    request_data = await generate_request_data(db_session, data_record, 3)
    response = backend_execute_plugin(backend_client, request_data, data_record['id'])
    validate_api_response(data_record, response, 200)
    await validate_data_source_checkin(db_session, response.json(), data_record, False)

    backend_delete_plugin(backend_client, plugin_api_str, data_record)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record)

@pytest.mark.asyncio
async def test_qdrant_execute_two_embedding(db_session, backend_client, qdrant_client,
                                            test_embedding, test_data_source):
    embedding_record1 = test_embedding()
    embedding_record2 = test_embedding()
    embedding_records = [embedding_record1, embedding_record2]
    data_record = test_data_source(embedding_records)
    qdrant_upsert(qdrant_client, data_record, embedding_records, 32)

    request_data = await generate_request_data(db_session, data_record, 3)
    response = backend_execute_plugin(backend_client, request_data, data_record['id'])
    validate_api_response(data_record, response, 200)
    await validate_data_source_checkin(db_session, response.json(), data_record, False)

    backend_delete_plugin(backend_client, plugin_api_str, data_record)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record1)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record2)

@pytest.mark.asyncio
async def test_qdrant_execute_wrong_embedding(db_session, backend_client, qdrant_client,
                                            test_embedding, test_data_source):
    
    embedding_record1 = test_embedding()
    data_record1 = test_data_source([embedding_record1])
    qdrant_upsert(qdrant_client, data_record1, [embedding_record1], 32)

    embedding_record2 = test_embedding()
    data_record2 = test_data_source([embedding_record2])
    qdrant_upsert(qdrant_client, data_record2, [embedding_record2], 32)

    request_data = await generate_request_data(db_session, data_record1, 3)
    response = backend_execute_plugin(backend_client, request_data, data_record2['id'])
    validate_api_response(data_record2, response, 200)
    for r in response.json():
        assert r['valid'] == False, r

    backend_delete_plugin(backend_client, plugin_api_str, data_record1)
    backend_delete_plugin(backend_client, plugin_api_str, data_record2)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record1)
    backend_delete_plugin(backend_client, plugin_api_str, embedding_record2)
