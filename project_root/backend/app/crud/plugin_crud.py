from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from typing import Union 

from app import schemas
from app.utils import handle_db_exception
from vvs_database import crud, execution

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

async def cleanup_unreferenced_items(db: AsyncSession):
    try:
        n_removed = await crud.cleanup_unreferenced_items(db)
        return n_removed 
    except Exception as e:
        handle_db_exception(e)

async def cleanup_unreferenced_jobs(db: AsyncSession):
    try:
        n_removed = await crud.cleanup_unreferenced_jobs(db)
        return n_removed 
    except Exception as e:
        handle_db_exception(e)

async def execute_plugin(db: AsyncSession,
                         plugin_id: int,
                         execute_request: Union[schemas.ExecuteRequestUnion, schemas.BatchExecuteRequestUnion],
                         execute_params: schemas.ExecuteParams
                         ):
    try:
        response = await execution.execute_plugin(db, 
                                                    plugin_id,
                                                    execute_request,
                                                    execute_params,
                                                    log_id='backend')
        return response 
    except AssertionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        handle_db_exception(e)

