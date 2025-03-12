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

@router.post("/{plugin_id:int}")
async def execute_plugin(plugin_id: int, 
                         execute_request: schemas.ExecuteRequestUnion,
                         cache: bool=False,
                         db_lookup: bool=False,
                         db_persist: bool=False,
                         use_semaphore: bool=True,
                         max_semaphore_attempts: int=20,
                         queue_polling_interval: float=0.2,
                         db: AsyncSession = Depends(get_db)
                         ):
    response = await crud.execute_plugin(db, plugin_id, execute_request, cache, db_lookup, db_persist,
                                         use_semaphore, max_semaphore_attempts, queue_polling_interval)
    return response 

@router.post("/{plugin_id:int}/batch")
async def batch_execute_plugin(plugin_id: int,
                               execute_request: schemas.BatchExecuteRequestUnion,
                               cache: bool=False,
                               db_lookup: bool=False,
                               db_persist: bool=False,
                               use_semaphore: bool=True,
                               max_semaphore_attempts: int=20,
                               queue_polling_interval: float=0.2,
                               db: AsyncSession = Depends(get_db)
                               ):
    response = await crud.execute_plugin(db, plugin_id, execute_request, cache, db_lookup, db_persist,
                                         use_semaphore, max_semaphore_attempts, queue_polling_interval)
    return response 

