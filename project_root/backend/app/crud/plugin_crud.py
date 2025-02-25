from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from typing import Union 

from app import schemas, utils
from vvs_database import crud
from vvs_database.exceptions import ValidationError, NotFoundError, ReferenceError
from pydantic import ValidationError as PydanticValidationError

def handle_db_exception(e):
    if isinstance(e, ValidationError) or isinstance(e, PydanticValidationError):
        raise HTTPException(status_code=422, detail=str(e))
    elif isinstance(e, NotFoundError):
        raise HTTPException(status_code=404, detail=str(e))
    elif isinstance(e, ReferenceError):
        raise HTTPException(status_code=400, detail=str(e))
    else:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def get_plugin(db: AsyncSession, 
                     plugin_id: int, 
                     with_exception: bool=True, 
                     response_model: bool=False):
    try:
        plugin = await crud.get_plugin(db, plugin_id, with_exception, response_model)
    except Exception as e:
        handle_db_exception(e)
    return plugin

async def create_plugin(db: AsyncSession, plugin: schemas.PluginCreate, response_model: bool=False):
    try:
        db_plugin = await crud.create_plugin(db=db, plugin=plugin, response_model=response_model)
        return db_plugin
    except Exception as e:
        handle_db_exception(e)

async def update_plugin(db: AsyncSession, plugin_id: int, plugin: schemas.PluginUpdate, response_model: bool=False):
    try:
        db_plugin = await crud.update_plugin(db, plugin_id, plugin, response_model)
        return db_plugin
    except Exception as e:
        handle_db_exception(e)

# async def execute_plugin(db_plugin, execute_request):
#     execution_type = db_plugin.execution_type.lower()

#     try:
#         if type(execute_request) == list:
#             utils.validate_execute_request(db_plugin, execute_request)
#             execution_function = utils.batch_execute_plugin_map.get(execution_type, None)
#         else:
#             utils.validate_execute_request(db_plugin, [execute_request])
#             execution_function = utils.execute_plugin_map.get(execution_type, None)
#     except ValidationError as e:
#         raise HTTPException(status_code=422, detail=str(e))
    
#     if execution_function is None:
#         raise HTTPException(status_code=400, detail=f"Execute plugin of type {db_plugin['type']} not supported")
#     else:
#         response = await execution_function(db_plugin, execute_request)
    
#     return response

async def execute_plugin(db: AsyncSession, plugin_id: int,
                         execute_request: Union[schemas.ExecuteRequestUnion, 
                                                schemas.BatchExecuteRequestUnion]):
    try:
        response = await crud.execute_plugin(db, plugin_id, execute_request)
        return response 
    except AssertionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        handle_db_exception(e)

