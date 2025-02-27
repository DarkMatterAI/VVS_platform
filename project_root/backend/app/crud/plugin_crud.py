from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from typing import Union 

from app import schemas
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

async def execute_plugin(db: AsyncSession, plugin_id: int,
                         execute_request: Union[schemas.ExecuteRequestUnion, 
                                                schemas.BatchExecuteRequestUnion],
                         checkin_result: bool=False):
    try:
        response = await crud.execute_plugin(db, plugin_id, execute_request, checkin_result)
        return response 
    except AssertionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        handle_db_exception(e)

async def cleanup_unreferenced_items(db: AsyncSession):
    try:
        await crud.cleanup_unreferenced_items(db)
    except Exception as e:
        handle_db_exception(e)

