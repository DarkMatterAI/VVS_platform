from sqlalchemy.ext.asyncio import AsyncSession

from typing import List 

from vvs_database import crud, logging, schemas  
from vvs_database.execution.ops import ItemOp
from vvs_database.execution.connections import Connections
from vvs_database.job_runner.base_runner import JobRunner

def get_configs(job_json: dict, plugins: dict):
    job_json = schemas.CreateQdrantUploadJob(**job_json)
    user_embedding_configs = {}
    if job_json.embedding_configs is not None:
        user_embedding_configs = {i.plugin_id:i for i in job_json.embedding_configs}

    configs = []
    for plugin_id, plugin in plugins.items():
        if plugin.type != 'embedding':
            continue 

        if plugin_id in user_embedding_configs:
            plugin_config = user_embedding_configs[plugin_id]
        else:
            plugin_config = schemas.ExecutePlugin(plugin_id=plugin_id,
                                                  execute_params=schemas.ExecuteParams(),
                                                  runtime_args=None)
            
        # disable for qdrant job
        plugin_config.execute_params.cache = False
        plugin_config.execute_params.db_persist = False
        plugin_config.execute_params.db_lookup = False 
        plugin_config.plugin = plugin 

        configs.append(plugin_config)
    return configs 

def record_to_item(record: dict) -> schemas.InternalItem:
    return schemas.InternalItem(item_data=schemas.ItemData(item_id=-1,
                                                           external_id=record['external_id'],
                                                           item=record['item']),
                                valid=True,
                                score=None,
                                embeddings={},
                                assembly_data=None,
                                query_group=None)

def records_to_items(records: list[dict]) -> List[schemas.InternalItem]:
    return [record_to_item(i) for i in records]


class QdrantUploadRunner(JobRunner):
    async def load_job(self, db_session: AsyncSession):
        await super().load_job(db_session)
        self.plugin_configs = get_configs(self.job.job_json, self.plugins)
        self.embedding_ids = [i.plugin.id for i in self.plugin_configs]
        self.collection_name = f"data_source_{self.job.job_json['plugin_id']}"

    async def save_failed(self, db_session, failed: List[dict]):
        logging.info(f'{self.log_id}: Saving {len(failed)} failed items')
        if failed:
            await crud.create_qdrant_upload_failures(db_session, self.job_id, failed)

    async def execute_item_ops(self, records: List[dict], connections: Connections):
        logging.info(f"{self.log_id}: Starting Qdrant upload embedding")
        connections.db_service.job_id = self.job_id
        items = records_to_items(records)

        for plugin_config in self.plugin_configs:
            item_op = ItemOp(plugin_config, [], connections, self.log_id)
            items = await item_op(items)

        outputs = []
        for item in items:
            output = {
                "item_data": {
                    "item": item.item_data.item,
                    "external_id": item.item_data.external_id
                },
                "valid": item.valid,
                "embeddings": {k:v.embedding for k,v in item.embeddings.items()}
            }
            outputs.append(output)
        return outputs 

