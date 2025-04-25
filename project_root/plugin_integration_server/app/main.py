from fastapi import FastAPI, HTTPException
from . import schemas
from .plugins.qdrant import QdrantPlugin
from .plugins.triton import TritonPlugin
from typing import List 

app = FastAPI()
# tei_plugin = TeiPlugin()
qdrant_plugin = QdrantPlugin()
triton_plugin = TritonPlugin()

@app.get("/")
async def read_root():
    return {"Hello": "World", "service" : "plugin_integration_server"}


@app.post("/data_source_qdrant/{collection_name}", response_model=schemas.DataSourceResponseUnion)
async def qdrant_data_source(collection_name: str, request: schemas.DataSourceRequestUnion):
     return await qdrant_plugin.process(request, collection_name=collection_name)

@app.post("/triton_embed/{model_name}", response_model=schemas.EmbedResponseUnion)
async def triton_embed(model_name: str, request: schemas.EmbedRequestUnion):
    return await triton_plugin.process(request, model_name=model_name)

@app.post("/triton_map/{model_name}", response_model=schemas.MapperResponseUnion)
async def triton_mapper(model_name: str, request: schemas.MapperRequestUnion):
    return await triton_plugin.process(request, model_name=model_name)

