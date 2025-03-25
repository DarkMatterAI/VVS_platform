import dagster as dg

from vvs_dagster.resources import PostgresResource

class GetPluginConfig(dg.Config):
    plugin_id: int 

@dg.op
async def get_plugin(context: dg.OpExecutionContext, 
                     postgres_resource: PostgresResource,
                     config: GetPluginConfig):
    db_service = postgres_resource.get_db_service()
    plugin = await db_service.get_plugin(config.plugin_id)
    context.log.info(f"plugin {config.plugin_id} : {plugin}")
    await db_service.db.close()

default_config = dg.RunConfig(
    ops={"get_plugin": GetPluginConfig(plugin_id=1)}
)

@dg.job(config=default_config)
def get_plugin_job():
    get_plugin()    

