from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from typing import Union 

from app import schemas
from vvs_database import crud, execution
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

async def execute_plugin(db: AsyncSession, 
                         plugin_id: int,
                         execute_request: Union[schemas.ExecuteRequestUnion, schemas.BatchExecuteRequestUnion],
                         cache: bool=False,
                         db_lookup: bool=False,
                         db_persist: bool=False,
                         use_semaphore: bool=True,
                         max_semaphore_attempts: int=20,
                         queue_polling_interval: float=0.2,
                         ):
    try:
        response = await execution.execute_plugin(db, 
                                                  plugin_id, 
                                                  execute_request, 
                                                  cache=cache,
                                                  db_lookup=db_lookup,
                                                  db_persist=db_persist,
                                                  use_semaphore=use_semaphore,
                                                  max_semaphore_attempts=max_semaphore_attempts,
                                                  queue_polling_interval=queue_polling_interval)
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

