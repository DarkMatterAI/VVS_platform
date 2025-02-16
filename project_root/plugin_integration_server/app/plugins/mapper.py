# from .base import BasePlugin
# from ..utils import post_request
# from ..config import PLUGIN_CONFIG

# class MapperPlugin(BasePlugin):
#     async def process(self, request):
#         embedding = request.embedding
#         mapper_data = {
#             "inputs": [
#                 {
#                     "name": "embedding",
#                     "shape": [1, len(embedding)],
#                     "datatype": "FP32",
#                     "data": [embedding]
#                 }
#             ]
#         }
#         response = await post_request(mapper_data, PLUGIN_CONFIG['mapper'])
#         n_out, d_out = response['outputs'][0]['shape'][1:]
#         output_embedding = response['outputs'][0]['data']
#         result = {'valid': True, 'embedding': []}

#         for i in range(n_out):
#             embedding = output_embedding[i*d_out:(i+1)*d_out]
#             result['embedding'].append(embedding)

#         return result