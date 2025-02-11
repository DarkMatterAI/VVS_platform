from fastapi import FastAPI, HTTPException
from . import schemas
from .plugins.tei import TeiPlugin
from .plugins.mapper import MapperPlugin
from .plugins.qdrant import QdrantPlugin

app = FastAPI()
tei_plugin = TeiPlugin()
mapper_plugin = MapperPlugin()
qdrant_plugin = QdrantPlugin()

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.post("/tei_embed", response_model=schemas.EmbedResponse)
async def tei_embed(request: schemas.EmbedRequest):
    return await tei_plugin.process(request)

@app.post("/data_source_qdrant/{collection_name}", response_model=schemas.DataSourceResponse)
async def qdrant_data_source(collection_name: str, request: schemas.DataSourceRequest):
    try:
        return await qdrant_plugin.process(collection_name, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while querying qdrant: {str(e)}")

@app.post("/mapper_plugin", response_model=schemas.MapperResponse)
async def mapper_plugin_endpoint(request: schemas.MapperRequest):
    return await mapper_plugin.process(request)
