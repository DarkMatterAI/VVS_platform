from .base import BasePlugin
from ..utils import post_request
from ..config import PLUGIN_CONFIG
from fastapi import HTTPException

triton_config = PLUGIN_CONFIG['triton']

class TritonPlugin(BasePlugin):
    def __init__(self):
        self.build_request_configs()

    def build_request_configs(self):
        self.request_configs = {}
        self.embedding_models = set(triton_config['model_names']['embedding'])
        self.mapper_models = set(triton_config['model_names']['mapper'])
        all_names = list(self.embedding_models) + list(self.mapper_models)
        for name in all_names:
            request_config = {
                'url' : f"{PLUGIN_CONFIG['triton']['base_url']}/{name}/infer",
                'timeout' : PLUGIN_CONFIG['triton']['timeout'],
                'retries' : PLUGIN_CONFIG['triton']['retries']
            }
            self.request_configs[name] = request_config

    async def process_embedding(self, request, model_name):
        request_data = {
            'inputs' : [
                {
                    'name' : 'sequence',
                    'shape' : [len(request), 1],
                    'datatype' : 'BYTES',
                    'data' : [i.item_data.item for i in request]
                    # 'data' : [i.item for i in request]
                }
            ]
        }
        response = await post_request(request_data, self.request_configs[model_name])
        data = response['outputs'][0]['data']
        n_out, d_out = response['outputs'][0]['shape']
        # output = [{'embedding' : data[i*d_out:(i+1)*d_out]} for i in range(n_out)]
        output = [{'embedding' : data[i*d_out:(i+1)*d_out], 'valid' : True} for i in range(n_out)]
        return output
    
    async def process_mapper(self, request, model_name):
        # embeddings = [i.embedding for i in request]
        embeddings = [i.embedding.embedding for i in request]
        mapper_data = {
            "inputs": [
                {
                    "name": "embedding",
                    "shape": [len(request), len(embeddings[0])],
                    "datatype": "FP32",
                    "data": embeddings
                }
            ]
        }
        response = await post_request(mapper_data, self.request_configs[model_name])
        data = response['outputs'][0]['data']
        bs, n_out, d_emb = response['outputs'][0]['shape']

        result = []
        for i in range(bs):
            r = {'valid' : True, 'embedding' : []}

            for j in range(n_out):
                embedding = data[i * n_out * d_emb + j * d_emb : i * n_out * d_emb + (j + 1) * d_emb]
                r['embedding'].append(embedding)
            result.append(r)
        return result 

    async def _process(self, request, model_name):
        if model_name in self.embedding_models:
            response = await self.process_embedding(request, model_name)
        elif model_name in self.mapper_models:
            response = await self.process_mapper(request, model_name)
        else:
            raise HTTPException(status_code=404, detail=f"Triton model {model_name} not found")

        return response 
