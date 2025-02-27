import os 
from fastapi import HTTPException
import asyncio 
import numpy as np 
import string 
from typing import List, Union  

from app.crud_records import create_records, delete_records

from vvs_database import schemas 

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))
MAX_BATCH_SIZE = int(os.environ.get('TEST_BATCH_SIZE', 5))

def embed_response(request: List[schemas.ItemRequest]):
    return [{'valid' : True, 
             'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()}
             for r in request]

def data_source_response(requests: List[schemas.DataSourceRequest]):
    batch_response = []

    for request in requests:
        query_embedding = np.array(request.embedding.embedding)
        response = {'valid' : True, 'result' : []}

        for i in range(request.k):
            embedding = np.random.randn(EMBEDDING_SIZE)
            distance = ((query_embedding - embedding)**2).sum()**0.5
            response['result'].append({
                'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
                'external_id' : np.random.randint(1e8),
                'embedding' : embedding.tolist(),
                'distance' : float(distance)
            })
        batch_response.append(response)
    return batch_response 

def filter_response(request: List[schemas.ItemRequest]):
    return [{'valid' : True} for r in request]

def score_response(request: List[schemas.ItemRequest]):
    return [{'valid' : True, 'score' : 10*np.random.rand()} for r in request]

def mapper_response(request: List[schemas.MapperRequest]):
    return [{'valid' : True, 
             'embedding' : [np.random.rand(EMBEDDING_SIZE).tolist() 
                            for i in range(NUM_PARENTS)]}
            for r in request]

def assembly_response(request: List[schemas.AssemblyRequest]):
    response = [{'valid' : True, 
                 'result' : [{'item' : ''.join([i.item for i in r.parents]), 
                              'external_id' : None}]}
                for r in request]
    return response 

response_mapping = {
    'embedding' : embed_response,
    'data_source' : data_source_response,
    'filter' : filter_response,
    'score' : score_response,
    'mapper' : mapper_response,
    'assembly' : assembly_response
}

async def get_response(plugin_type, request):
    delist = False 
    if type(request) != list:
        request = [request]
        delist = True 

    if len(request) > MAX_BATCH_SIZE:
        raise HTTPException(status_code=422, detail=f"batch size limit")
    
    response = response_mapping[plugin_type](request)

    if delist:
        response = response[0]

    await asyncio.sleep(0)
    return response 

