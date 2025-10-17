import os 
import numpy as np

test_server_url = f"http://test_server:{os.environ['TEST_SERVER_PORT']}"
# test_server_url = f"http://localhost:{os.environ['TEST_SERVER_PORT']}"
EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))
MAX_BATCH_SIZE = int(os.environ.get('TEST_BATCH_SIZE', 5))


def get_create_data(plugin_type):

    mock_data = {
        "name" : f"mock_{plugin_type}_api_{np.random.randint(1e4)}",
        "plugin_class" : "generic",
        "type" : plugin_type,
        "execution_type" : "api",
        "group_key" : "mock_api",
        "timeout" : 4,
        "max_concurrency" : 4,
        "batch_size" : MAX_BATCH_SIZE,
        "max_retries" : 2,
        "endpoint_url" : f"{test_server_url}/{plugin_type}",
    }
    return mock_data 

def get_mock_embed():
    mock_data = get_create_data('embedding')
    mock_data['vector_length'] = EMBEDDING_SIZE
    mock_data['distance_metric'] = "Euclid"
    return mock_data 

def get_mock_data_source(embedding_ids):
    mock_data = get_create_data('data_source')
    mock_data['embedding_ids'] = [i['id'] for i in embedding_ids]
    return mock_data 

def get_mock_filter():
    mock_data = get_create_data('filter')
    return mock_data 

def get_mock_score():
    mock_data = get_create_data('score')
    return mock_data 

def get_mock_mapper(input_embedding, output_embeddings):
    mock_data = get_create_data('mapper')
    mock_data['input_embedding_id'] = input_embedding['id']
    mock_data['output_order'] = [{'index' : i, 'embedding_id' : output_embeddings[i]['id']}
                                  for i in range(len(output_embeddings))]
    return mock_data

def get_mock_assemble():
    mock_data = get_create_data('assembly')
    mock_data['num_parents'] = NUM_PARENTS
    return mock_data 

mock_mapping = {
    'embedding' : get_mock_embed,
    'data_source' : get_mock_data_source,
    'filter' : get_mock_filter,
    'score' : get_mock_score,
    'mapper' : get_mock_mapper,
    'assembly' : get_mock_assemble
}
