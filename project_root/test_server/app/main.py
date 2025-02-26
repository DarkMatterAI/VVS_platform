import os 
from fastapi import FastAPI, HTTPException
import asyncio 
import numpy as np 
import string 
from typing import List, Union  

from app.crud_records import create_records, delete_records

from vvs_database import schemas 

from contextlib import contextmanager

app = FastAPI()

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))
MAX_BATCH_SIZE = int(os.environ.get('TEST_BATCH_SIZE', 5))

@contextmanager
def record_management():
    records = create_records()
    try:
        yield records
    finally:
        delete_records(records)

@app.on_event("startup")
def startup_event():
    app.record_manager = record_management()
    app.records = app.record_manager.__enter__()

@app.on_event("shutdown")
def shutdown_event():
    app.record_manager.__exit__(None, None, None)

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.post("/embedding", response_model=schemas.EmbedResponse)
async def embed(request: schemas.ItemRequest):
    response = {'valid' : True, 'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()}
    await asyncio.sleep(0)
    return response 

@app.post("/data_source", response_model=schemas.DataSourceResponse)
async def data_source(request: schemas.DataSourceRequest):

    query_embedding = np.array(request.embedding.embedding)
    response = {
        'valid' : True,
        'result' : []
    }

    for i in range(request.k):
        embedding = np.random.randn(EMBEDDING_SIZE)
        distance = ((query_embedding - embedding)**2).sum()**0.5
        response['result'].append({
            'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
            'external_id' : np.random.randint(1e8),
            'embedding' : embedding.tolist(),
            'distance' : float(distance)
        })

    await asyncio.sleep(0)
    return response 

@app.post("/filter", response_model=Union[schemas.FilterResponse, List[schemas.FilterResponse]])
async def filter(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    if type(request) == list:
        if len(request) > MAX_BATCH_SIZE:
            raise HTTPException(status_code=422, detail=f"batch size limit")
        response = [{'valid' : True} for i in request]
    else:
        response = {'valid' : True}
    await asyncio.sleep(0)
    return response 

@app.post("/score", response_model=schemas.ScoreResponse)
async def score(request: schemas.ItemRequest):
    response = {'valid' : True, 'score' : 10*np.random.rand()}
    await asyncio.sleep(0)
    return response 

@app.post("/mapper", response_model=schemas.MapperResponse)
async def mapper(request: schemas.MapperRequest):
    response = {'valid' : True, 'embedding' : [np.random.rand(EMBEDDING_SIZE).tolist() for i in range(NUM_PARENTS)]}
    await asyncio.sleep(0)
    return response 

@app.post("/assembly", response_model=schemas.AssemblyResponse)
async def assemble(request: schemas.AssemblyRequest):
    response = {
        'valid' : True,
        'result' : [{'item' : ''.join([i.item for i in request.parents]), 'external_id' : None}]
    }
    await asyncio.sleep(0)
    return response 
