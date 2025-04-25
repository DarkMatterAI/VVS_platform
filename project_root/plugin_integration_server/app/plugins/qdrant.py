# import os
# import numpy as np
# from contextlib import asynccontextmanager
# from qdrant_client import AsyncQdrantClient, models
# from fastapi import HTTPException
# from .base import BasePlugin

# class QdrantPlugin(BasePlugin):
#     @asynccontextmanager
#     async def get_client(self):
#         client = AsyncQdrantClient(
#             location='qdrant',
#             port=int(os.environ.get('QDRANT__SERVICE__HTTP_PORT', 6333)),
#             grpc_port=int(os.environ.get('QDRANT__SERVICE__GRPC_PORT', 6334)),
#             prefer_grpc=True,
#             timeout=60
#         )
#         try:
#             yield client
#         finally:
#             await client.close()

#     async def _process(self, request, collection_name):
#         async with self.get_client() as client:

#             collection_info = await client.get_collection(collection_name)
#             vector_names = collection_info.config.params.vectors.keys()
#             search_queries = []
#             embedding_names = []

#             for r in request:
#                 embedding_name = f"embedding_{r.embedding.plugin_id}"

#                 if embedding_name not in vector_names:
#                     raise HTTPException(status_code=422, detail=f"Query embedding {embedding_name} " \
#                             f"not found in collection {collection_name}, expected one of {list(vector_names)}")

#                 query = models.QueryRequest(query=r.embedding.embedding,
#                                             using=embedding_name,
#                                             limit=r.k,
#                                             with_vector=True,
#                                             with_payload=True
#                                             )
#                 search_queries.append(query)
#                 embedding_names.append(embedding_name)


#             try:
#                 qdrant_results = await client.query_batch_points(
#                     collection_name=collection_name,
#                     requests=search_queries,
#                 )
#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"An error occurred while querying qdrant: {str(e)}")

#             results_batch = []
#             for i, result in enumerate(qdrant_results):
#                 results = []
#                 for point in result.points:
#                     embedding = point.vector[embedding_names[i]]
#                     norm = point.payload.get('norm', None)
#                     if norm is not None:
#                         embedding = (np.array(embedding) * norm).tolist()

#                     result_data = {
#                         'external_id': point.payload.get('external_id', 0),
#                         'item': point.payload.get('item', ''),
#                         'embedding': embedding,
#                         'distance': point.score
#                     }
#                     results.append(result_data)
#                 results_batch.append({'valid' : bool(results), 'result' : results})

#             return results_batch
