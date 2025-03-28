import dagster as dg

from vvs_dagster.resources import PostgresResource

from vvs_database import crud 

class GetPluginConfig(dg.Config):
    plugin_id: int 

class GetJobConfig(dg.Config):
    job_id: int 

@dg.op
async def get_plugin(context: dg.OpExecutionContext, 
                     postgres_resource: PostgresResource,
                     config: GetPluginConfig):
    db_service = postgres_resource.get_service()
    plugin = await db_service.get_plugin(config.plugin_id)
    context.log.info(f"plugin {config.plugin_id} : {plugin}")
    await db_service.db.close()

default_config = dg.RunConfig(
    ops={"get_plugin": GetPluginConfig(plugin_id=1)}
)

@dg.job(config=default_config)
def get_plugin_job():
    get_plugin()    



@dg.op
async def get_job(context: dg.OpExecutionContext, 
                  postgres_resource: PostgresResource,
                  config: GetJobConfig):
    db_session = postgres_resource.get_db()
    job = await crud.get_job(db_session, config.job_id, load_plugins=True)
    # db_service = postgres_resource.get_service()
    # job = await db_service.get_job(config.job_id, load_plugins=True)
    context.log.info(f"job {config.job_id} : {job}")
    for jp in job.plugins:
        context.log.info(f"job plugin {jp}")
        context.log.info(f"plugin id {jp.plugin_id}")

        plugin = jp.plugin
        context.log.info(f"plugin {plugin}")
        await db_session.refresh(plugin, ['embeddings'])
        for embedding in jp.plugin.embeddings:
            context.log.info(f"embedding {embedding}")
            context.log.info(f"embedding id {embedding.id}")
    await db_session.commit()
    await db_session.close()
    return job 

@dg.op
def inspect_job(context: dg.OpExecutionContext, 
                job):
    context.log.info(f"job {job}")
    for jp in job.plugins:
        context.log.info(f"job plugin {jp}")
        context.log.info(f"plugin id {jp.plugin_id}")
        context.log.info(f"plugin {jp.plugin}")
        for embedding in jp.plugin.embeddings:
            context.log.info(f"embedding {embedding}")
            context.log.info(f"embedding id {embedding.id}")

default_config_job = dg.RunConfig(
    ops={"get_job": GetJobConfig(job_id=1)}
)

@dg.job(config=default_config_job)
def get_job_job():
    job = get_job()
    inspect_job(job=job)
