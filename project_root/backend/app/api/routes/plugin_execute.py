from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas, utils
from app import crud 
from app.core.database import get_db 

router = APIRouter()

@router.post("/{plugin_id:int}")
async def execute_plugin(plugin_id: int, 
                         execute_request: schemas.ExecuteRequestUnion,
                         db: AsyncSession = Depends(get_db)
                         ):
    print(f'getting record for plugin {plugin_id}')
    db_plugin = await crud.get_plugin(db, plugin_id=plugin_id)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    
    print(f'executing plugin {plugin_id}')
    response = await crud.execute_plugin(db_plugin, execute_request)
    return response 

@router.get("/{result_id}")
async def get_result(result_id: str, delete: bool=True):
    response = await utils.get_redis_result(result_id, delete)
    return response 

@router.post("/{plugin_id:int}/batch")
async def batch_execute_plugin(plugin_id: int,
                               execute_request: schemas.BatchExecuteRequestUnion,
                               db: AsyncSession = Depends(get_db)
                               ):
    print(f'getting record for plugin {plugin_id}')
    db_plugin = await crud.get_plugin(db, plugin_id=plugin_id)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    
    print(f'batch executing plugin {plugin_id}')
    response = await crud.execute_plugin(db_plugin, execute_request)
    return response 

@router.post("/result_batch")
async def get_result_batch(result_ids: list[schemas.RedisResult], delete: bool=True):
    response = await utils.get_redis_result_batch(result_ids, delete)
    return response 
