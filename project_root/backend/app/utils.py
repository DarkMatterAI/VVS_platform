import httpx 
from fastapi import HTTPException
import yaml
from vvs_database.utils import make_post_request
from vvs_database.exceptions import ValidationError, NotFoundError, ReferenceError
from pydantic import ValidationError as PydanticValidationError

def read_config():
    """Read application configuration from YAML file."""
    with open('app/launch_config.yaml', 'r') as file:
        return yaml.safe_load(file)
    
async def fastapi_post_request(data, url, timeout, retries, retry_sleep=0):
    """
    A wrapper around make_post_request that converts exceptions to FastAPI HTTPExceptions.
    
    Args:
        data: JSON serializable data to send in the request
        url: The URL to send the request to
        timeout: Timeout in seconds
        retries: Number of retries to attempt
        retry_sleep: Sleep time between retries in seconds
        
    Returns:
        The JSON response from the server
        
    Raises:
        HTTPException: If any error occurs during the request
    """
    try:
        return await make_post_request(data, url, timeout, retries, retry_sleep)
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        raise HTTPException(status_code=status_code, detail=str(e))
    except httpx.TimeoutException as e:
        raise HTTPException(
            status_code=504,  # Gateway Timeout
            detail=str(e)
        )
    except httpx.ConnectError as e:
        raise HTTPException(
            status_code=503,  # Service Unavailable
            detail=str(e)
        )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,  # Internal Server Error
            detail=str(e)
        )
    except Exception as e:
        # Catch-all for any other unexpected exceptions
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )

def handle_db_exception(e):
    if isinstance(e, ValidationError) or isinstance(e, PydanticValidationError):
        raise HTTPException(status_code=422, detail=str(e))
    elif isinstance(e, NotFoundError):
        raise HTTPException(status_code=404, detail=str(e))
    elif isinstance(e, ReferenceError):
        raise HTTPException(status_code=400, detail=str(e))
    else:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

