from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, List, Optional
from redis.asyncio import Redis

from app import schemas
from app import crud 
from app.core.database import get_db, get_redis_client

router = APIRouter()

@router.get("/summary", response_model=Dict[str, int])
async def get_plugins_summary(db: AsyncSession = Depends(get_db)):
    return await crud.get_plugins_summary(db)

@router.post("/", response_model=schemas.PluginInDBUnion)
async def create_plugin(plugin: schemas.PluginCreate, db: AsyncSession = Depends(get_db)):
    response = await crud.create_plugin(db=db, plugin=plugin.root, response_model=True)
    return response 

@router.get("/{plugin_id:int}", response_model=schemas.PluginInDBUnion)
async def read_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.get_plugin(db, plugin_id=plugin_id, response_model=True)
    return response 

@router.get("/", response_model=List[schemas.PluginInDBUnion])
async def scroll_plugins(plugin_type: Optional[schemas.PluginType]=None, 
                         plugin_class: Optional[schemas.PluginClass]=None,
                         name: Optional[str] = Query(None),
                         group_key: Optional[str] = Query(None),
                         skip: int = 0, 
                         limit: int = 100, 
                         db: AsyncSession = Depends(get_db)):
    
    filter_params = {
        'type' : plugin_type,
        'plugin_class' : plugin_class,
        'group_key' : group_key,
        'name' : name 
    }
    filter_params = {k: v for k, v in filter_params.items() if v is not None}
    
    plugins = await crud.get_plugins(db, filter_params=filter_params, 
                                     skip=skip, limit=limit, response_model=True)
    return plugins 

@router.put("/{plugin_id:int}", response_model=schemas.PluginInDBUnion)
async def update_plugin(plugin_id: int, plugin: schemas.PluginUpdate, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.update_plugin(db=db, plugin_id=plugin_id, plugin=plugin, response_model=True)
    return db_plugin

@router.delete("/{plugin_id:int}", response_model=schemas.PluginInDBUnion)
async def delete_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.delete_plugin(db=db, plugin_id=plugin_id)
    return db_plugin

@router.delete("/clear_cache/{plugin_id:int}")
async def clear_plugin_cache(plugin_id: int, redis_client = Depends(get_redis_client)):
    n_deleted = await crud.clear_plugin_cache(plugin_id, redis_client)
    response = {'success' : True, 'removed' : n_deleted}
    return response 

@router.delete("/clear_semaphores/{plugin_id:int}")
async def clear_plugin_semaphores_endpoint(
    plugin_id: int,
    redis_client: Redis = Depends(get_redis_client),
    job_ids: Optional[List[int]] = Query(None, description="Optional list of job IDs to limit pruning"),
    scan_count: int = Query(1000, ge=10, le=10000),
):
    stats = await crud.clear_plugin_semaphores(
        plugin_id=plugin_id,
        redis_client=redis_client,
        job_ids=job_ids,
        scan_count=scan_count,
    )
    return {"success": True, "plugin_id": plugin_id, **stats}