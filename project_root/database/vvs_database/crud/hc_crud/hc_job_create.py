from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from copy import deepcopy 

from vvs_database.utils import get_plugin_response_model
from vvs_database import logging 
from vvs_database.exceptions import ValidationError

from vvs_database.crud.plugin_crud import get_plugin 
from vvs_database.crud.item_checkin import item_checkin
from vvs_database.crud.job_crud import create_job, bulk_create_job_plugins

from vvs_database.models import HCInputItems, Job

from vvs_database.schemas.enums import JobType, PluginType
from vvs_database.schemas.item_schemas import NewItem
from vvs_database.schemas.hc_schemas import (HCSearchConfigs, 
                                             HCAssembedInputItem,
                                             HCInferenceParams,
                                             HCCreateConfigs)


async def load_search_config_plugins(db: AsyncSession, 
                                     search_config: HCSearchConfigs):
    embeddings = {}
    for plugin_config in search_config.iter_plugins():
        plugin_record = await get_plugin(db, plugin_config.plugin_id, with_error=True)
        await db.refresh(plugin_record, ["embeddings"])
        plugin = get_plugin_response_model(plugin_record)
        plugin_config.plugin = plugin
        
        for k,v in plugin_config.override_params.model_dump().items():
            if v is not None:
                logging.info(f"Overriding plugin {plugin_config.plugin_id} parameter {k}:{v}")
                setattr(plugin_config.plugin, k, v)
        
        if plugin.type == 'embedding':
            embeddings[plugin.id] = plugin
        else:
            for embedding_record in plugin_record.embeddings:
                embedding = get_plugin_response_model(embedding_record)
                embeddings[embedding.id] = embedding

    validate_search_config_plugin_types(search_config)
                
    search_config.update_embedding_dict(embeddings)
    search_config.update_data_configs()
    search_config.update_source_embeddings()
        
    return search_config

def validate_search_config_plugin_types(search_config: HCSearchConfigs):
    plugin_type_iter = [
        (search_config.mapper_config, PluginType.MAPPER),
        (search_config.assembly_config, PluginType.ASSEMBLY),
        (search_config.data_configs, PluginType.DATA_SOURCE),
        (search_config.filter_configs, PluginType.FILTER),
        (search_config.score_config, PluginType.SCORE)
    ]

    for plugin_configs, plugin_type in plugin_type_iter:
        if plugin_configs is None:
            continue 
            
        elif type(plugin_configs) != list:
            plugin_configs = [plugin_configs]

        for config in plugin_configs:
            assert config.plugin.type == plugin_type 

def validtate_hc_mapper_config(search_config: HCSearchConfigs):
    mapper_config = search_config.mapper_config
    data_configs = search_config.data_configs
    if mapper_config is None:
        return
    
    n_mapper_out = len(mapper_config.plugin.output_order)        
    n_data_configs = len(data_configs)
    
    if n_data_configs != n_mapper_out:
        raise ValidationError(f"Expected {n_mapper_out} data configs to match mapper, found {n_data_configs}")
        
    mapper_output_embeddings = {i.index : i.embedding_id for i in mapper_config.plugin.output_order}
    
    for data_config in data_configs:
        assembly_index = data_config.data_source_params.assembly_index
        data_embeddings = data_config.plugin.embedding_ids
        embedding_id = mapper_output_embeddings[assembly_index]
        if embedding_id not in data_embeddings:
            raise ValidationError(f"Mapper {mapper_config.plugin_id} output embedding {embedding_id} " \
                                  f"with assembly index {assembly_index} routed to data source " \
                                  f"{data_config.plugin_id} which expects one of {data_embeddings}")
        
def validate_hc_assembly_config(search_config: HCSearchConfigs):
    assembly_config = search_config.assembly_config
    data_configs = search_config.data_configs
    if assembly_config is None:
        return 
    
    n_parents = assembly_config.plugin.num_parents
    n_data_configs = len(data_configs)
    
    if n_data_configs != n_parents:
        raise ValidationError(f"Expected {n_parents} data configs to match assembly, found {n_data_configs}")

# async def hc_inputs_checkin(db: AsyncSession, 
#                             job_inputs: List[HCAssembedInputItem]):
#     new_items = []
#     new_item_idxs = []
#     external_ids = []
#     output = {}
#     for input_idx, input_item in enumerate(job_inputs):
#         output[input_idx] = {
#             'job_args' : {'inference_limit' : input_item.inference_limit, 
#                           'time_limit' : input_item.time_limit,
#                           'max_iterations' : input_item.max_iterations},
#             'job_inputs' : {}
#         }
#         for item in input_item.items:
#             new_items.append(NewItem(**item.model_dump()))
#             new_item_idxs.append((input_idx, item.assembly_index))
#             external_ids.append(item.external_id)
            
#     new_items = await item_checkin(db, new_items, None)
#     new_items = new_items['items']
#     for new_item_idxs, external_id, new_item in zip(new_item_idxs, external_ids, new_items):
#         input_idx, assembly_idx = new_item_idxs
#         output[input_idx]['job_inputs'][assembly_idx] = {'item' : new_item,
#                                                          'external_id' : external_id}
    
#     return output

async def hc_inputs_checkin(db: AsyncSession, 
                            job_inputs: List[HCAssembedInputItem]):
    new_items = []
    new_item_idxs = []
    external_ids = []
    output = {}
    for input_idx, input_item in enumerate(job_inputs):
        inf = input_item.inference_params or HCInferenceParams()
        output[input_idx] = {
            "job_args" : {"inference_limit": inf.inference_limit, 
                          "time_limit":      inf.time_limit,
                          "max_iterations":  input_item.max_iterations},
            "update_params": input_item.update_params.model_dump() if input_item.update_params else None,
            "job_inputs": {}
        }
        for item in input_item.items:
            new_items.append(NewItem(**item.model_dump()))
            new_item_idxs.append((input_idx, item.assembly_index))
            external_ids.append(item.external_id)
            
    new_items = await item_checkin(db, new_items, None)
    new_items = new_items["items"]
    for new_item_idxs, external_id, new_item in zip(new_item_idxs, external_ids, new_items):
        input_idx, assembly_idx = new_item_idxs
        output[input_idx]["job_inputs"][assembly_idx] = {"item": new_item,
                                                         "external_id": external_id}
    
    return output

async def create_hc_parent_job(db: AsyncSession, 
                               create_config: HCCreateConfigs, 
                               search_config: HCSearchConfigs
                               ) -> Job:
    job_params = create_config.job_params
    extra_args = job_params.model_dump()
    auto_execute = extra_args.pop('auto_execute')
    job = await create_job(db,
                           job_type=JobType.HILL_CLIMB_JOB,
                           job_json=create_config.model_dump(),
                           auto_execute=job_params.auto_execute,
                           extra_args=extra_args)
    plugin_ids = []
    for plugin_config in search_config.iter_plugins():
        plugin_ids.append(plugin_config.plugin_id)
        
    for plugin_id in search_config.embedding_dict.keys():
        plugin_ids.append(plugin_id)
        
    plugin_ids = list(set(plugin_ids))
    job_plugins = await bulk_create_job_plugins(db, job.id, plugin_ids)
    
    return job

async def create_hc_input_item(db: AsyncSession, 
                               job: Job, 
                               job_inputs):
    input_items = []
    for assembly_idx, item in job_inputs['job_inputs'].items():
        external_id = item['external_id']
        item = item['item']
        input_item = HCInputItems(job_id=job.id,
                                  item_id=item.id,
                                  assembly_index=assembly_idx,
                                  external_id=external_id)
        db.add(input_item)
        input_items.append(input_item)
    return input_items 
    
# async def create_hc_input_job(db: AsyncSession, 
#                               item_dict: dict, 
#                               parent_job: Job, 
#                               job_json: dict):
#     input_jobs = []
#     for idx, input_data in item_dict.items():
#         job_args = input_data['job_args']
#         job_args['parent_id'] = parent_job.id
#         job_inputs = input_data['job_inputs']
        
#         job = await create_job(db,
#                                job_type=JobType.HILL_CLIMB_JOB_INPUT,
#                                job_json=job_json,
#                                auto_execute=parent_job.auto_execute,
#                                extra_args=job_args)
#         input_jobs.append(job)
#         input_items = await create_hc_input_item(db, job, input_data)
#     await db.commit()
        
#     return input_jobs 

async def create_hc_input_job(db: AsyncSession, 
                              item_dict: dict, 
                              parent_job: Job, 
                              job_json: dict):
    input_jobs = []
    for idx, input_data in item_dict.items():
        job_args   = input_data["job_args"]
        job_args["parent_id"] = parent_job.id

        # base template comes from parent (identical search cfg)
        child_json = deepcopy(job_json)

        # ── override update params if present on this input ─────────
        if input_data.get("update_params") is not None:
            child_json["update_params"] = input_data["update_params"]

        job = await create_job(
             db,
             job_type=JobType.HILL_CLIMB_JOB_INPUT,
            job_json=child_json,
             auto_execute=parent_job.auto_execute,
             extra_args=job_args,
         )
        input_jobs.append(job)
        input_items = await create_hc_input_item(db, job, input_data)
    await db.commit()
        
    return input_jobs 

async def create_hc_job(db: AsyncSession, 
                        create_config: HCCreateConfigs):  
    update_params = create_config.update_params.convert_internal()
    job_inputs = [i.convert_internal() for i in create_config.job_inputs]
    
    # load config records
    plugin_config = create_config.plugin_config
    search_config = HCSearchConfigs.from_create_config(plugin_config)
    job_json = {'search_config' : search_config.model_dump(),
                'update_params' : update_params.model_dump()}
    search_config = await load_search_config_plugins(db, search_config)
    
    # validate config records
    validtate_hc_mapper_config(search_config)
    validate_hc_assembly_config(search_config)
    
    # check in inputs 
    item_dict = await hc_inputs_checkin(db, job_inputs)
        
    # create parent job 
    parent_job = await create_hc_parent_job(db, create_config, search_config)

    # create input jobs 
    input_jobs = await create_hc_input_job(db, item_dict, parent_job, job_json)
    
    return search_config, item_dict, parent_job, input_jobs


