import os 
from fastapi import FastAPI
import asyncio 
import numpy as np 
import string 

from app import schemas 
from app.crud_records import create_records, delete_records

from contextlib import contextmanager

app = FastAPI()

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))
NUM_PARENTS = int(os.environ.get('TEST_NUM_PARENTS', 2))

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
async def embed(request: schemas.EmbedRequest):
    response = {'embedding' : np.random.randn(EMBEDDING_SIZE).tolist()}
    await asyncio.sleep(0)
    return response 

@app.post("/data_source", response_model=schemas.DataSourceResponse)
async def data_source(request: schemas.DataSourceRequest):

    query_embedding = np.array(request.embedding[0].embedding)
    response = {
        'valid' : True,
        'result' : []
    }

    for i in range(request.k):
        embedding = np.random.randn(EMBEDDING_SIZE)
        distance = ((query_embedding - embedding)**2).sum()**0.5
        response['result'].append({
            'external_id' : np.random.randint(1e8),
            'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
            'embedding' : [embedding.tolist()],
            'distance' : [float(distance)]
        })

    await asyncio.sleep(0)
    return response 

@app.post("/filter", response_model=schemas.FilterResponse)
async def filter(request: schemas.ItemRequest):
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
