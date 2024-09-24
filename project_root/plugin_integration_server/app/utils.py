import os 
import httpx 
import time 
from fastapi import HTTPException

from contextlib import asynccontextmanager
from qdrant_client import AsyncQdrantClient

async def post_request(data, plugin_config, retry_sleep=1):
    url = plugin_config['url']
    timeout = plugin_config['timeout']
    retries = plugin_config['retries']
    async with httpx.AsyncClient() as client:
        for attempt in range(retries + 1):
            print(f"Post Request to {url} attempt {attempt+1}")
            try:
                response = await client.post(url, json=data, timeout=timeout )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if attempt == retries:
                    raise HTTPException(status_code=e.response.status_code, detail=str(e))
            except httpx.RequestError as e:
                if attempt == retries:
                    raise HTTPException(status_code=500, detail=f"An error occurred while requesting the API: {str(e)}")
            except Exception as e:
                if attempt == retries:
                    raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
                
            if retry_sleep>0:
                print(f"Post request failed, sleeping")
                time.sleep(retry_sleep)

    raise HTTPException(status_code=500, detail="Failed to execute API plugin after maximum retries")


@asynccontextmanager
async def get_qdrant_client():
    client = AsyncQdrantClient(location='qdrant', 
                               port=int(os.environ.get('QDRANT__SERVICE__HTTP_PORT', 6333)), 
                               grpc_port=int(os.environ.get('QDRANT__SERVICE__GRPC_PORT', 6334)),
                               prefer_grpc=True,
                               timeout=60
                               )
    try:
        yield client
    finally:
        await client.close()


async def qdrant_query(collection_name, request):
    async with get_qdrant_client() as client:
        embedding_name = f"embedding_{request['embedding']['id']}"
        qdrant_results = await client.query_points(
            collection_name=collection_name,
            query=request['embedding']['embedding'],
            using=embedding_name,
            limit=request['k'],
            with_vectors=True
        )

        results = [] 
        for result in qdrant_results.points:
            result_data = {
                'external_id' : result.payload.get('external_id', 0),
                'item' : result.payload.get('item', ''),
                'embedding' : result.vector[embedding_name],
                'distance' : result.score
            }
            results.append(result_data)
        return results



# async def execute_qdrant_plugin(plugin: models.Plugin, execute_request: dict):
#     execute_request = execute_request.model_dump()
#     try:
#         response = await qdrant_query(plugin, execute_request)
#         return response 
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred while querying qdrant: {str(e)}")




