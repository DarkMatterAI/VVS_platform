# from fastapi import APIRouter, Depends, HTTPException, Query
# from sqlalchemy.ext.asyncio import AsyncSession
# from typing import Dict, List 

# from app import schemas
# from app.crud import item_crud as crud 
# from app.crud.plugin_crud import get_plugin
# from app.core.database import get_db 

# router = APIRouter()


# @router.get("/{item_id}", response_model=schemas.Item)
# async def get_item(item_id: int, db: AsyncSession = Depends(get_db)):
#     item = await crud.get_item(db, item_id)
#     if not item:
#         raise HTTPException(status_code=404, detail="Item not found")
#     return item

# @router.delete("/{item_id}", response_model=schemas.Item)
# async def delete_item(item_id: int, db: AsyncSession = Depends(get_db)):
#     item = await crud.delete_item(db, item_id)
#     if item is None:
#         raise HTTPException(status_code=404, detail="Item not found")
#     return item

# @router.get("/{item_id}/sources/{plugin_id}", response_model=schemas.ItemSource)
# async def get_item_source(item_id: int, plugin_id: int, db: AsyncSession = Depends(get_db)):
#     source = await crud.get_item_source(db, item_id, plugin_id)
#     if not source:
#         raise HTTPException(status_code=404, detail="Item source not found")
#     return source

# @router.delete("/{item_id}/sources/{plugin_id}")
# async def delete_item_source(item_id: int, plugin_id: int, db: AsyncSession = Depends(get_db)):
#     source = await crud.delete_item_source(db, item_id, plugin_id)
#     if not source:
#         raise HTTPException(status_code=404, detail="Item source not found")
#     return source

# @router.post("/cleanup", response_model=Dict[str, int])
# async def cleanup_items(db: AsyncSession = Depends(get_db)):
#     deleted_count = await crud.cleanup_unreferenced_items(db)
#     return {"deleted_count": deleted_count}

# @router.post("/item_checkin", response_model=schemas.ItemCheckinResponse)
# async def item_checkin(
#     items: List[schemas.NewItem],
#     plugin_id: int = Query(..., description="Source plugin ID"),
#     db: AsyncSession = Depends(get_db)
# ):
#     plugin = await get_plugin(db, plugin_id)
#     if plugin is None:
#         raise HTTPException(status_code=404, detail="Plugin not found")
#     return await crud.item_checkin(db, items, plugin_id)

# @router.get("/{item_id}/scores/{plugin_id}", response_model=schemas.ItemScore)
# async def get_item_score(item_id: int, plugin_id: int, db: AsyncSession = Depends(get_db)):
#     source = await crud.get_item_score(db, item_id, plugin_id)
#     if not source:
#         raise HTTPException(status_code=404, detail="Item source not found")
#     return source

# @router.delete("/{item_id}/scores/{plugin_id}")
# async def delete_item_score(item_id: int, plugin_id: int, db: AsyncSession = Depends(get_db)):
#     source = await crud.delete_item_score(db, item_id, plugin_id)
#     if not source:
#         raise HTTPException(status_code=404, detail="Item source not found")
#     return source

# @router.post("/score_checkin", response_model=List[schemas.ItemScore])
# async def score_checkin(
#     items: List[schemas.NewScore],
#     plugin_id: int = Query(..., description="Source plugin ID"),
#     db: AsyncSession = Depends(get_db)
# ):
#     plugin = await get_plugin(db, plugin_id)
#     if plugin is None:
#         raise HTTPException(status_code=404, detail="Plugin not found")
#     return await crud.score_checkin(db, items, plugin_id)
