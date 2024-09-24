from sqlalchemy.ext.asyncio import AsyncSession

from .plugin_crud import (create_plugin, 
                          get_plugin, 
                          get_plugins,
                          update_plugin, 
                          execute_plugin,
                          get_plugins_summary
                          )
from .plugin_crud import delete_plugin as delete_plugin_generic
from .qdrant_crud import delete_qdrant

async def delete_plugin(db: AsyncSession, plugin_id: int):
    db_plugin = await get_plugin(db, plugin_id)
    if not db_plugin:
        return None
    
    if db_plugin.execution_type == 'internal_qdrant':
        response = await delete_qdrant(db, db_plugin)
    else:
        response = await delete_plugin_generic(db, db_plugin)
    
    return response 

