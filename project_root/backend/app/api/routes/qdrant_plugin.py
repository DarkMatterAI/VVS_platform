from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import utils, schemas 
from app.crud import qdrant_crud
from app.crud import plugin_crud as crud
from app.core.database import get_db 


router = APIRouter()

@router.post("/", response_model=schemas.PluginInDBUnion)
async def create_plugin(plugin: schemas.QdrantDataSourceCreate, db: AsyncSession = Depends(get_db)):
    record_id = await qdrant_crud.create_qdrant(db, plugin)
    # sqlalcehmy throws greenlet error if `get_plugin` is inside `qdrant_crud.create`
    response = await crud.get_plugin(db, record_id)
    return utils.get_plugin_response_model(response)

@router.get("/update_collection_data/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def update_collection_data(plugin_id: int, db: AsyncSession = Depends(get_db)):
    record_id = await qdrant_crud.update_collection_data(db, plugin_id)
    if record_id is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    # sqlalcehmy throws greenlet error if `get_plugin` is inside `qdrant_crud.update_collection_data`
    db_plugin = await crud.get_plugin(db, record_id)
    return utils.get_plugin_response_model(db_plugin)

@router.post("/update_snapshot/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def update_snapshot(plugin_id: int, snapshot_data: schemas.QdrantSnapshotData, db: AsyncSession = Depends(get_db)):
    record_id = await qdrant_crud.update_snapshot(db, plugin_id, snapshot_data)
    if record_id is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    db_plugin = await crud.get_plugin(db, plugin_id)
    print(db_plugin.config)
    return utils.get_plugin_response_model(db_plugin)

