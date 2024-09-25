import os 
from fastapi import FastAPI, HTTPException

from . import schemas, utils 

app = FastAPI()

PLUGIN_CONFIG = {
    'tei' : {
        'url' : f"http://tei_plugin:{os.environ.get('TEI_PORT', '')}/embed",
        'timeout' : 120,
        'retries' : 3
    },
    'mapper' : {
        'url' : f"http://mapper_plugin:{os.environ.get('TRITON_HTTP_PORT', '')}/v2/models/Mapper/infer",
        'timeout' : 120,
        'retries' : 3
    }
}

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.post("/tei_embed", response_model=schemas.EmbedResponse)
async def tei_embed(request: schemas.EmbedRequest):
    tei_data = {'inputs' : request.item,
                'normalize' : False if os.environ.get('TEI_NORMALIZE', 'false')=='false' else True,
                'truncate' : False if os.environ.get('TEI_TRUNCATE', 'false')=='false' else True,
                'truncation_direction' : os.environ.get('TEI_TRUNCATION_DIRECTION', 'right')
                }
    response = await utils.post_request(tei_data, PLUGIN_CONFIG['tei'])
    response = {'embedding' : response[0]}
    return response 

@app.post("/data_source_qdrant/{collection_name}", response_model=schemas.DataSourceResponse)
async def qdrant_data_source(collection_name: str, request: schemas.DataSourceRequest):
    try:
        response = await utils.qdrant_query(collection_name, request)
        return response 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while querying qdrant: {str(e)}")

@app.post("/mapper_plugin", response_model=schemas.MapperResponse)
async def mapper_plugin(request: schemas.MapperRequest):
    embedding = request.embedding
    mapper_data = {
        "inputs" : [
            {
                "name" : "embedding",
                "shape" : [1, len(embedding)],
                "datatype" : "FP32",
                "data" : [embedding]
            }
        ]
    }
    response = await utils.post_request(mapper_data, PLUGIN_CONFIG['mapper'])
    n_out, d_out = response['outputs'][0]['shape'][1:]
    output_embedding = response['outputs'][0]['data']
    response = {'valid' : True, 'embedding' : []}

    for i in range(n_out):
        embedding = output_embedding[i*d_out:(i+1)*d_out]
        response['embedding'].append(embedding)

    return response 
