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
                    'shape' : [1, 1],
                    'datatype' : 'BYTES',
                    'data' : [request.item]
                }
            ]
        }
        response = await post_request(request_data, self.request_configs[model_name])
        return {'embedding' : response['outputs'][0]['data']}
    
    async def process_mapper(self, request, model_name):
        embedding = request.embedding
        mapper_data = {
            "inputs": [
                {
                    "name": "embedding",
                    "shape": [1, len(embedding)],
                    "datatype": "FP32",
                    "data": [embedding]
                }
            ]
        }
        response = await post_request(mapper_data, self.request_configs[model_name])
        n_out, d_out = response['outputs'][0]['shape'][1:]
        output_embedding = response['outputs'][0]['data']
        result = {'valid': True, 'embedding': []}

        for i in range(n_out):
            embedding = output_embedding[i*d_out:(i+1)*d_out]
            result['embedding'].append(embedding)

        return result
    
    async def process(self, request, model_name):
        if model_name in self.embedding_models:
            response = await self.process_embedding(request, model_name)
        elif model_name in self.mapper_models:
            response = await self.process_mapper(request, model_name)
        else:
            raise HTTPException(status_code=404, detail=f"Triton model {model_name} not found")

        return response 
