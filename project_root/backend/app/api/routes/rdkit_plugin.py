from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud 
from app.core.database import get_db 
from app.schemas.rdkit_plugin_schemas import (RDKitPluginCreate,
                                              RDKitPluginUpdate,
                                              PluginInDBUnion
                                              )


router = APIRouter()

@router.post("/", response_model=PluginInDBUnion)
async def create_plugin(plugin: RDKitPluginCreate, db: AsyncSession = Depends(get_db)):
    print(plugin.root)
    response = await crud.create_plugin(db=db, plugin=plugin.root, response_model=True)
    return response 

@router.put("/{plugin_id}", response_model=PluginInDBUnion)
async def update_plugin(plugin_id: int, plugin: RDKitPluginUpdate, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.update_plugin(db=db, plugin_id=plugin_id, plugin=plugin, response_model=True)
    return db_plugin
