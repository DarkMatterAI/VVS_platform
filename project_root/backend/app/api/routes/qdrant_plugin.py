from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import utils, schemas 
from app.crud import qdrant_crud as crud 
from app.crud import plugin_crud 
from app.core.database import get_db 


router = APIRouter()

@router.post("/", response_model=schemas.PluginInDBUnion)
async def create_plugin(plugin: schemas.QdrantDataSourceCreate, db: AsyncSession = Depends(get_db)):
    record_id = await crud.create(db, plugin)
    # sqlalcehmy throws greenlet error if `get_plugin` is inside `qdrant_crud.create`
    response = await plugin_crud.get_plugin(db, record_id)
    return utils.get_plugin_response_model(response)

@router.get("/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def read_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    db_plugin = await plugin_crud.get_plugin(db, plugin_id=plugin_id)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return utils.get_plugin_response_model(db_plugin)

@router.delete("/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def delete_plugin(plugin_id: int, db: AsyncSession = Depends(get_db)):
    db_plugin = await crud.delete(db=db, plugin_id=plugin_id)
    if db_plugin is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return db_plugin
