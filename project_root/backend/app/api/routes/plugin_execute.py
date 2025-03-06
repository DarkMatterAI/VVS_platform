from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app import crud 
from app.core.database import get_db 

router = APIRouter()

@router.get("/item_cleanup")
async def item_cleanup(db: AsyncSession = Depends(get_db)):
    await crud.cleanup_unreferenced_items(db)
    response = {'success' : True}
    return response

# @router.post("/{plugin_id:int}")
# async def execute_plugin(plugin_id: int, 
#                          execute_request: schemas.ExecuteRequestUnion,
#                          checkin_result: bool=False,
#                          db: AsyncSession = Depends(get_db)
#                          ):
#     response = await crud.execute_plugin(db, plugin_id, execute_request, checkin_result)
#     return response 

@router.post("/{plugin_id:int}")
async def execute_plugin(plugin_id: int, 
                         execute_request: schemas.ExecuteRequestUnion,
                         cache: bool=False,
                         db_lookup: bool=False,
                         db_persist: bool=False,
                         db: AsyncSession = Depends(get_db)
                         ):
    response = await crud.execute_plugin(db, plugin_id, execute_request, cache, db_lookup, db_persist)
    return response 

# @router.get("/{result_id}")
# async def get_result(result_id: str, delete: bool=True):
#     response = await crud.get_redis_result(result_id, delete)
#     return response 

# @router.post("/{plugin_id:int}/batch")
# async def batch_execute_plugin(plugin_id: int,
#                                execute_request: schemas.BatchExecuteRequestUnion,
#                                checkin_result: bool=False,
#                                db: AsyncSession = Depends(get_db)
#                                ):
#     response = await crud.execute_plugin(db, plugin_id, execute_request, checkin_result)
#     return response 

@router.post("/{plugin_id:int}/batch")
async def batch_execute_plugin(plugin_id: int,
                               execute_request: schemas.BatchExecuteRequestUnion,
                               cache: bool=False,
                               db_lookup: bool=False,
                               db_persist: bool=False,
                               db: AsyncSession = Depends(get_db)
                               ):
    response = await crud.execute_plugin(db, plugin_id, execute_request, cache, db_lookup, db_persist)
    return response 

# @router.post("/result_batch")
# async def get_result_batch(result_ids: list[schemas.RedisResult], delete: bool=True):
#     response = await crud.get_redis_result_batch(result_ids, delete)
#     return response 
