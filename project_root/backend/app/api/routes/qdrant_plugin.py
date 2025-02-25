from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas, crud 
from app.core.database import get_db 


router = APIRouter()

@router.post("/", response_model=schemas.PluginInDBUnion)
async def create_plugin(plugin: schemas.QdrantDataSourceCreate, db: AsyncSession = Depends(get_db)):
    record_id = await crud.qdrant_crud.create_qdrant(db, plugin)
    # sqlalcehmy throws greenlet error if `get_plugin` is inside `qdrant_crud.create`
    response = await crud.get_plugin(db, plugin_id=record_id, response_model=True)
    return response 

@router.get("/update_collection_data/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def update_collection_data(plugin_id: int, db: AsyncSession = Depends(get_db)):
    record_id = await crud.qdrant_crud.update_collection_data(db, plugin_id)
    if record_id is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    # sqlalcehmy throws greenlet error if `get_plugin` is inside `qdrant_crud.update_collection_data`
    response = await crud.get_plugin(db, plugin_id=record_id, response_model=True)
    return response 

@router.post("/update_snapshot/{plugin_id}", response_model=schemas.PluginInDBUnion)
async def update_snapshot(plugin_id: int, snapshot_data: schemas.QdrantSnapshotData, db: AsyncSession = Depends(get_db)):
    record_id = await crud.qdrant_crud.update_snapshot(db, plugin_id, snapshot_data)
    if record_id is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    response = await crud.get_plugin(db, plugin_id=record_id, response_model=True)
    return response 

@router.get("/orphan_collections")
async def get_orphan_collections(db: AsyncSession = Depends(get_db)):
    response = await crud.qdrant_utils.get_orphan_collections(db)
    return response 

@router.delete("/orphan_collections")
async def delete_orphan_collections(db: AsyncSession = Depends(get_db)):
    response = await crud.qdrant_utils.delete_orphan_collections(db)
    return response 

