from fastapi import FastAPI, HTTPException
from . import schemas
from .plugins.qdrant import QdrantPlugin
from typing import List 

app = FastAPI()
qdrant_plugin = QdrantPlugin()

@app.get("/")
async def read_root():
    return {"Hello": "World", "service" : "plugin_integration_server"}

@app.post("/data_source_qdrant/{collection_name}", response_model=schemas.DataSourceResponseUnion)
async def qdrant_data_source(collection_name: str, request: schemas.DataSourceRequestUnion):
     return await qdrant_plugin.process(request, collection_name=collection_name)
