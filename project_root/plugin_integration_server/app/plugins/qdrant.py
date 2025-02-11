import os
import numpy as np
from contextlib import asynccontextmanager
from qdrant_client import AsyncQdrantClient
from .base import BasePlugin

class QdrantPlugin(BasePlugin):
    @asynccontextmanager
    async def get_client(self):
        client = AsyncQdrantClient(
            location='qdrant',
            port=int(os.environ.get('QDRANT__SERVICE__HTTP_PORT', 6333)),
            grpc_port=int(os.environ.get('QDRANT__SERVICE__GRPC_PORT', 6334)),
            prefer_grpc=True,
            timeout=60
        )
        try:
            yield client
        finally:
            await client.close()

    async def process(self, collection_name, request):
        request_dict = request.model_dump()
        async with self.get_client() as client:
            embedding_name = f"embedding_{request_dict['embedding_id']}"
            qdrant_results = await client.query_points(
                collection_name=collection_name,
                query=request_dict['embedding'],
                using=embedding_name,
                limit=request_dict['k'],
                with_vectors=True
            )

            results = []
            for result in qdrant_results.points:
                embedding = result.vector[embedding_name]
                norm = result.payload.get('norm', None)
                if norm is not None:
                    embedding = (np.array(embedding) * norm).tolist()

                result_data = {
                    'external_id': result.payload.get('external_id', 0),
                    'item': result.payload.get('item', ''),
                    'embedding': embedding,
                    'distance': result.score
                }
                results.append(result_data)

            return {'valid': bool(results), 'result': results}
