from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, List, Optional

from app import schemas, utils
from app import crud 
from app.core.database import get_db 

router = APIRouter()

@router.get("/summary", response_model=Dict[str, int])
async def get_plugins_summary(db: AsyncSession = Depends(get_db)):
    return await crud.get_plugins_summary(db)

@router.post("/", response_model=schemas.PluginInDBUnion)
async def create_plugin(plugin: schemas.PluginCreate, db: AsyncSession = Depends(get_db)):
    response = await crud.create_plugin(db=db, plugin=plugin.root, response_model=True)
    return response 
    # return utils.get_plugin_response_model(response)

@router.get("/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def read_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    response = await crud.get_plugin(db, plugin_id=plugin_id, response_model=True)
    return response 
    # return utils.get_plugin_response_model(db_plugin)

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
    # return [utils.get_plugin_response_model(plugin) for plugin in plugins]

@router.put("/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def update_plugin(plugin_id: int, plugin: schemas.PluginUpdate, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.update_plugin(db=db, plugin_id=plugin_id, plugin=plugin, response_model=True)
    return db_plugin
    # db_plugin = await crud.update_plugin(db=db, plugin_id=plugin_id, plugin=plugin)
    # if db_plugin is None:
        # raise HTTPException(status_code=404, detail="Plugin not found")
    # return utils.get_plugin_response_model(db_plugin)

@router.delete("/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def delete_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.delete_plugin(db=db, plugin_id=plugin_id)
    # if db_plugin is None:
        # raise HTTPException(status_code=404, detail="Plugin not found")
    return db_plugin
