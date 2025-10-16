from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import JSONResponse

from vvs_database.crud import (
    get_plugins,
    get_plugins_summary,
    count_plugins_by_class,
    count_plugins_linked_to_embedding_class,
    delete_plugin_from_model,
    get_jobs,
    upload_file,
    delete_file
)

from vvs_database.crud.hc_crud import (
    create_hc_job,
    fetch_hc_job_results,
    count_hc_job_results,
    export_hc_job_hierarchy
)

from app.crud import qdrant_crud, qdrant_utils
from app.crud.plugin_crud import (
    handle_db_exception,
    get_plugin,
    create_plugin,
    update_plugin,
    execute_plugin,
    cleanup_unreferenced_items,
    cleanup_unreferenced_jobs,
)

from app.crud.job_crud import (
    get_job,
    delete_job,
    kill_job
)

from app.crud.redis_crud import (
    clear_plugin_cache,
    clear_plugin_semaphores,
    clear_job_semaphores
)

# from vvs_database.utils import clear_plugin_cache

__all__ = [
    "get_plugin", 
    "get_plugins",
    "get_plugins_summary",
    "count_plugins_by_class", 
    "count_plugins_linked_to_embedding_class",
    "get_jobs",
    "upload_file",
    "delete_file",
    "create_plugin",
    "update_plugin",
    "execute_plugin",
    "cleanup_unreferenced_items",
    "cleanup_unreferenced_jobs",
    "delete_plugin",
    "get_job",
    "delete_job",
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
    except ReferenceError as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=409)
    except Exception as e:
        handle_db_exception(e)


