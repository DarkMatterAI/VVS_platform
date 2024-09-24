import os 
from fastapi import FastAPI

from . import schemas, utils 

app = FastAPI()

PLUGIN_CONFIG = {
    'tei' : {
        'url' : f"http://tei_plugin:{os.environ.get('TEI_PORT', '')}/embed",
        'timeout' : 120,
        'retries' : 3
    }
}

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.post("/tei_embed", response_model=schemas.EmbedResponse)
async def tei_embed(request: schemas.EmbedRequest):
    tei_data = {'inputs' : request.item}
    response = await utils.post_request(tei_data, PLUGIN_CONFIG['tei'])
    response = {'embedding' : response[0]}
    return response 

