from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from typing import List, Optional, Dict, Any

from app import schemas, utils
from vvs_database import crud
from vvs_database.exceptions import ValidationError, NotFoundError, ReferenceError
from pydantic import ValidationError as PydanticValidationError

def handle_db_exception(e):
    if isinstance(e, ValidationError) or isinstance(e, PydanticValidationError):
        raise HTTPException(status_code=422, detail=str(e))
    elif isinstance(e, NotFoundError):
        raise HTTPException(status_code=404, detail=str(e))
    elif isinstance(e, ReferenceError):
        raise HTTPException(status_code=400, detail=str(e))
    else:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def validate_embedding_ids(db: AsyncSession, embedding_ids: List[int]) -> None:
    try:
        await crud.validate_embedding_ids(db, embedding_ids)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

async def get_plugin(db: AsyncSession, plugin_id: int):
    plugin = await crud.get_plugin(db, plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail=f"Plugin with ID {plugin_id} not found")
    return plugin

async def get_plugins(
    db: AsyncSession, 
    filter_params: Dict[str, Any] = None,
    skip: int = 0, 
    limit: int = 100
):
    return await crud.get_plugins(db, filter_params, skip, limit)

async def create_plugin(db: AsyncSession, plugin: schemas.PluginCreate):
    try:
        plugin_data = plugin.model_dump(exclude={'embedding_ids', 'input_embedding_id'})
        
        embedding_ids = []
        if isinstance(plugin, schemas.MapperPluginCreate):
            embedding_order = [i.model_dump() for i in plugin.output_order]
            plugin_data['output_order'] = embedding_order
            plugin_data['input_embedding_id'] = plugin.input_embedding_id
        elif hasattr(plugin, 'embedding_ids') and (plugin.embedding_ids is not None):
            embedding_ids = plugin.embedding_ids
            
        db_plugin = await crud.create_plugin(
            db=db,
            plugin_type=plugin.type,
            plugin_data=plugin_data,
            embedding_ids=embedding_ids
        )
        return db_plugin
    except Exception as e:
        handle_db_exception(e)

async def update_plugin(db: AsyncSession, plugin_id: int, plugin: schemas.PluginUpdate):
    try:
        update_data = plugin.model_dump(exclude_unset=True)
        
        # Let utils validate the updates
        try:
            db_plugin = await get_plugin(db, plugin_id)
            utils.validate_updates(db_plugin, update_data)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=str(e))
            
        # Perform the update
        db_plugin = await crud.update_plugin(db, plugin_id, update_data)
        return db_plugin
    except Exception as e:
        handle_db_exception(e)

async def get_plugins_summary(db: AsyncSession):
    return await crud.get_plugins_summary(db)

async def count_plugins_by_class(db: AsyncSession, plugin_class):
    return await crud.count_plugins_by_class(db, plugin_class)

async def count_plugins_linked_to_embedding_id(db: AsyncSession, embedding_id: int) -> int:
    return await crud.count_plugins_linked_to_embedding_id(db, embedding_id)

async def count_plugins_linked_to_embedding_class(db: AsyncSession, plugin_class) -> int:
    return await crud.count_plugins_linked_to_embedding_class(db, plugin_class)

async def execute_plugin(db_plugin, execute_request):
    # Keep this in the backend layer as it's purely API-related
    execution_type = db_plugin.execution_type.lower()

    try:
        if type(execute_request) == list:
            utils.validate_execute_request(db_plugin, execute_request)
            execution_function = utils.batch_execute_plugin_map.get(execution_type, None)
        else:
            utils.validate_execute_request(db_plugin, [execute_request])
            execution_function = utils.execute_plugin_map.get(execution_type, None)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
    if execution_function is None:
        raise HTTPException(status_code=400, detail=f"Execute plugin of type {db_plugin['type']} not supported")
    else:
        response = await execution_function(db_plugin, execute_request)
    
    return response

