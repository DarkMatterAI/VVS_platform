from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.crud import (
    get_plugins,
    get_plugins_summary,
    count_plugins_by_class,
    count_plugins_linked_to_embedding_class,
    delete_plugin_from_model
)

from app.crud import qdrant_crud, qdrant_utils
from app.crud.plugin_crud import (
    handle_db_exception,
    get_plugin,
    create_plugin,
    update_plugin,
    execute_plugin,
    cleanup_unreferenced_items
)

from app.crud.job_crud import get_job

__all__ = [
    "get_plugin", 
    "get_plugins",
    "get_plugins_summary",
    "count_plugins_by_class", 
    "count_plugins_linked_to_embedding_class",
    "create_plugin",
    "update_plugin",
    "execute_plugin",
    "cleanup_unreferenced_items",
    "delete_plugin",
    "get_job"
]

async def delete_plugin(db: AsyncSession, plugin_id: int):
    db_plugin = await get_plugin(db, plugin_id)
    if not db_plugin:
        return None
    
    if db_plugin.plugin_class == 'internal_qdrant':
        delete_func = qdrant_crud.delete_qdrant
    else:
        delete_func = delete_plugin_from_model

    try:
        response = await delete_func(db, db_plugin)
        return response 
    except Exception as e:
        handle_db_exception(e)


