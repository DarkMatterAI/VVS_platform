import os 
from fastapi import FastAPI
from typing import List, Union  

from app.crud_records import create_records, delete_records
from app.execute_data import get_response

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

@app.post("/embedding", response_model=Union[schemas.EmbedResponse, List[schemas.EmbedResponse]])
async def embed(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    response = await get_response('embedding', request)
    return response 

@app.post("/data_source", response_model=Union[schemas.DataSourceResponse, List[schemas.DataSourceResponse]])
async def data_source(request: Union[schemas.DataSourceRequest, List[schemas.DataSourceRequest]]):
    response = await get_response('data_source', request)
    return response 

@app.post("/filter", response_model=Union[schemas.FilterResponse, List[schemas.FilterResponse]])
async def filter(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    response = await get_response('filter', request)
    return response 

@app.post("/score", response_model=Union[schemas.ScoreResponse, List[schemas.ScoreResponse]])
async def score(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    response = await get_response('score', request)
    return response 

@app.post("/mapper", response_model=Union[schemas.MapperResponse, List[schemas.MapperResponse]])
async def mapper(request: Union[schemas.MapperRequest, List[schemas.MapperRequest]]):
    response = await get_response('mapper', request)
    return response 

@app.post("/assembly", response_model=Union[schemas.AssemblyResponse, List[schemas.AssemblyResponse]])
async def assemble(request: Union[schemas.AssemblyRequest, List[schemas.AssemblyRequest]]):
    response = await get_response('assembly', request)
    return response 
