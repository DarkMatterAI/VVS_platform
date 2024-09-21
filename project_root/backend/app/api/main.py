from fastapi import APIRouter
from app import utils  
from app.api.routes import plugin_crud, plugin_execute, core

api_router = APIRouter()
api_router.include_router(plugin_crud.router, prefix="/plugins", tags=["plugins"])
api_router.include_router(plugin_execute.router, prefix="/execute", tags=["execute"])
api_router.include_router(core.router)

config = utils.read_config()['plugins']
if config.get('rdkit_plugin', {}).get('enabled', False):
    from app.api.routes import rdkit_plugin
    api_router.include_router(rdkit_plugin.router, prefix="/rdkit_plugins", tags=["rdkit_plugins"])

