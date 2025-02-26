import os 
import numpy as np 
import string 

EMBEDDING_SIZE = int(os.environ.get('TEST_SERVER_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))

def embed(request):
    # response = {'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()}
    response = {'valid' : True, 'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()}
    return response

def data_source(request):
    query_embedding = np.array(request['embedding']['embedding'])
    response = {
        'valid' : True,
        'result' : []
    }

    for i in range(request['k']):
        embedding = np.random.randn(EMBEDDING_SIZE)
        distance = ((query_embedding - embedding)**2).sum()**0.5
        response['result'].append({
            'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
            'external_id' : np.random.randint(1e8),
            'embedding' : embedding.tolist(),
            'distance' : float(distance)
        })

    return response 

    # query_embedding = np.array(request['embedding'])
    # response = {
    #     'valid' : True,
    #     'result' : []
    # }

    # for i in range(request['k']):
    #     embedding = np.random.randn(EMBEDDING_SIZE)
    #     distance = ((query_embedding - embedding)**2).sum()**0.5
    #     response['result'].append({
    #         'external_id' : np.random.randint(1e8),
    #         'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
    #         'embedding' : embedding.tolist(),
    #         'distance' : float(distance)
    #     })

    # return response 

def filter_op(request):
    return {'valid' : True}

def score(request):
    return {'valid' : True, 'score' : 10*np.random.rand()}

def mapper(request):
    response = {'valid' : True, 'embedding' : [np.random.rand(EMBEDDING_SIZE).tolist() for i in range(NUM_PARENTS)]}
    return response

def assembly(request):
    response = {
        'valid' : True,
        'result' : [{'item' : ''.join([i['item'] for i in request['parents']]), 'external_id' : None}]
    }
    return response

func_mapping = {
    'embedding' : embed,
    'data_source' : data_source,
    'filter' : filter_op,
    'score' : score,
    'mapper' : mapper,
    'assembly' : assembly
}
