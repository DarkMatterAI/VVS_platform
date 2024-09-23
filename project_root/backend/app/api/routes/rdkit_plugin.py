from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import utils  
from app.crud import plugin_crud as crud 
from app.core.database import get_db 
from app.schemas.rdkit_plugin_schemas import (RDKitFilterCreate, 
                                              RDKitFilterUpdate,
                                              FilterPluginInDB,
                                              RDKitPluginCreate,
                                              RDKitPluginUpdate,
                                              PluginInDBUnion
                                              )


router = APIRouter()

@router.post("/", response_model=PluginInDBUnion)
async def create_plugin(plugin: RDKitPluginCreate, db: AsyncSession = Depends(get_db)):
    print(plugin.root)
    response = await crud.create_plugin(db=db, plugin=plugin.root)
    return utils.get_plugin_response_model(response)

@router.get("/{plugin_id}", response_model=PluginInDBUnion)
async def read_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.get_plugin(db, plugin_id=plugin_id)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return utils.get_plugin_response_model(db_plugin)

@router.put("/{plugin_id}", response_model=PluginInDBUnion)
async def update_plugin(plugin_id: int, plugin: RDKitPluginUpdate, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.update_plugin(db=db, plugin_id=plugin_id, plugin=plugin)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return utils.get_plugin_response_model(db_plugin)

@router.delete("/{plugin_id}", response_model=PluginInDBUnion)
async def delete_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.delete_plugin(db=db, plugin_id=plugin_id)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return db_plugin


