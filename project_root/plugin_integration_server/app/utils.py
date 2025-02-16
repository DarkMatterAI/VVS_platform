import httpx
import time
from fastapi import HTTPException

async def post_request(data, plugin_config, retry_sleep=1):
    url = plugin_config['url']
    timeout = plugin_config['timeout']
    retries = plugin_config['retries']
    
    async with httpx.AsyncClient() as client:
        for attempt in range(retries + 1):
            print(f"Post Request to {url} attempt {attempt+1}")
            try:
                response = await client.post(url, json=data, timeout=timeout)
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
            
            if retry_sleep > 0:
                print(f"Post request failed, sleeping")
                time.sleep(retry_sleep)

    raise HTTPException(status_code=500, detail="Failed to execute API plugin after maximum retries")

# async def post_request(data, plugin_config, retry_sleep=1):
#     url = plugin_config['url']
#     timeout = plugin_config['timeout']
#     retries = plugin_config['retries']
#     response = await _post_request(data, url, timeout, retries, retry_sleep)
#     return response 

# async def _post_request(data, url, timeout, retries, retry_sleep=1):
    
#     async with httpx.AsyncClient() as client:
#         for attempt in range(retries + 1):
#             print(f"Post Request to {url} attempt {attempt+1}")
#             try:
#                 response = await client.post(url, json=data, timeout=timeout)
#                 response.raise_for_status()
#                 return response.json()
#             except httpx.HTTPStatusError as e:
#                 if attempt == retries:
#                     raise HTTPException(status_code=e.response.status_code, detail=str(e))
#             except httpx.RequestError as e:
#                 if attempt == retries:
#                     raise HTTPException(status_code=500, detail=f"An error occurred while requesting the API: {str(e)}")
#             except Exception as e:
#                 if attempt == retries:
#                     raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
            
#             if retry_sleep > 0:
#                 print(f"Post request failed, sleeping")
#                 time.sleep(retry_sleep)

#     raise HTTPException(status_code=500, detail="Failed to execute API plugin after maximum retries")
