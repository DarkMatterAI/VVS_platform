from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, List, Tuple 
from collections import defaultdict 
import asyncio

from vvs_database.crud import (
    get_plugin, 
    get_item_results,
    get_item_sources,
    get_assemblies_by_component_keys,
    item_checkin,
    result_checkin,
    assembly_checkin,
    upsert_execution_failures
)

from vvs_database.schemas import (
    ItemRequest,
    ItemResponseUnion,
    AssemblyRequest,
    AssemblyResponse,
    ExecuteRequestUnion, 
    ExecuteResponseUnion,
    NewItem,
    NewResult,
    NewAssembly,
    PostgresConnection
)
from vvs_database.models import Plugin
from vvs_database import logging 

class DatabaseService:
    """Service for database operations related to plugin execution"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.log_id = 'DB'

    async def get_plugin(self, plugin_id: int) -> Plugin:
        response = await get_plugin(self.db, plugin_id)
        return response 
    
    async def get_item_results(self, 
                               plugin: Plugin, 
                               requests: Dict[str, ItemRequest]
                               ) -> Dict[str, ItemResponseUnion]:
        logging.info(f"{self.log_id}: Looking up item results for {len(requests.keys())} requests")
        plugin_id = plugin.id 
        item_ids = [r.item_data.item_id for r in requests.values()]
        records = await get_item_results(self.db, item_ids, plugin_id)
        id_to_record = {r.item_id:r for r in records}

        result = {}
        for key, request in requests.items():
            record = id_to_record.get(request.item_data.item_id, None)
            if record is None:
                continue 

            result[key] = {"valid": record.valid,
                           "score": record.score,
                           "embedding": record.embedding}
        return result 

    async def get_assembly_results(self, 
                                   plugin: Plugin, 
                                   requests: Dict[str, AssemblyRequest]
                                   ) -> Dict[str, AssemblyResponse]:
        logging.info(f"{self.log_id}: Looking up assembly results for {len(requests.keys())} requests")
        # get component keys
        key_to_component = {}
        for key, request in requests.items():
            key_to_component[key] = request.generate_component_key(plugin.id)

        # look up records by component key
        assemblies = await get_assemblies_by_component_keys(self.db, list(key_to_component.values()))

        # group results by component key
        component_to_assembly = {}
        for assembly in assemblies:
            component_to_assembly.setdefault(assembly.component_key, []).append(assembly)

        # get external id 
        all_product_ids = {assembly.product_id for assembly in assemblies}
        product_sources = await get_item_sources(self.db, all_product_ids, plugin.id)
        item_id_to_external_id = {source.item_id: source.external_id for source in product_sources}

        # map assemblies/external ids back to requests 
        results = {}
        for key, request in requests.items():
            component_key = key_to_component[key]
            matching_assemblies = component_to_assembly.get(component_key, [])

            if not matching_assemblies:
                continue 

            assembly_results = []
            for assembly in matching_assemblies:
                product_item = assembly.product 

                external_id = item_id_to_external_id.get(product_item.id)

                assembly_results.append({"item": product_item.item,
                                         "external_id": external_id})
                
            results[key] = assembly_results 

        return results 

    async def check_in_item_results(self,
                                    plugin: Plugin,
                                    requests: List[ExecuteRequestUnion],
                                    results: List[ExecuteResponseUnion],
                                    valid_execution: List[bool]
                                    ):
        logging.info(f"{self.log_id}: Checking in {len(requests)} item results")
        new_results = []

        for request, response, valid_ex in zip(requests, results, valid_execution):
            if not valid_ex:
                continue 

            result_data = {"item_id": request.item_data.item_id,
                           "valid": response.valid,
                           "score": getattr(response, "score", None),
                           "embedding": getattr(response, "embedding", None)} 
            new_results.append(NewResult(**result_data))

        checkin_result = None 
        if new_results:
            checkin_result = await result_checkin(self.db, new_results, plugin.id)
        return checkin_result
    
    async def check_in_data_source_results(self,
                                           plugin: Plugin,
                                           requests: List[ExecuteRequestUnion],
                                           results: List[ExecuteResponseUnion],
                                           valid_execution: List[bool],
                                           persist: bool=False
                                           ):
        logging.info(f"{self.log_id}: Checking in {len(requests)} data source results")
        new_items = []
        embeddings = defaultdict(list)
        checkin_result = None
        for request, response, valid_ex in zip(requests, results, valid_execution):
            if not valid_ex:
                continue 

            if response.valid and response.result:
                for item in response.result:
                    external_id = item.external_id
                    if external_id is not None:
                        external_id = str(external_id)
                    new_items.append(NewItem(item=item.item,
                                             external_id=external_id))
                    embeddings[request.embedding.plugin_id].append(
                                      {'valid' : True, 
                                       'score' : None, 
                                       'embedding' : item.embedding,
                                       'item' : item.item 
                                       })

        if new_items:
            checkin_result = await item_checkin(self.db, new_items, plugin.id)
            item_to_id = {i.item : i.id for i in checkin_result['items']}
            for response in results:
                if response.valid and response.result:
                    for item in response.result:
                        item.item_id = item_to_id.get(item.item, None)

        if persist and (checkin_result is not None):
            item_records = checkin_result['items']
            item_records_dict = {i.item: i for i in item_records}

            for embedding_plugin_id, records in embeddings.items():
                new_results = []
                for record in records:
                    record['item_id'] = item_records_dict[record['item']].id
                    record = NewResult(**record)
                    new_results.append(record)

                _ = await result_checkin(self.db, new_results, embedding_plugin_id)

        return checkin_result
    
    async def check_in_assembly_results(self,
                                        plugin: Plugin,
                                        requests: List[ExecuteRequestUnion],
                                        results: List[ExecuteResponseUnion],
                                        valid_execution: List[bool],
                                        ):
        logging.info(f"{self.log_id}: Checking in {len(requests)} assembly results")
        new_assemblies = []
        checkin_result = None

        for request, response, valid_ex in zip(requests, results, valid_execution):
            if not valid_ex:
                continue 
            
            if (not response.valid) or (response.result is None) or (len(response.result)==0):
                continue 

            for result in response.result:
                components = [
                    {"item_id": parent.item_id, "assembly_index": parent.assembly_index}
                    for parent in request.parents
                ]
                external_id = result.external_id
                if external_id is not None:
                    external_id = str(external_id)
                new_assemblies.append(NewAssembly(item=result.item,
                                                  external_id=external_id,
                                                  components=components))
                    
        if new_assemblies:
            checkin_result = await assembly_checkin(self.db, new_assemblies, plugin.id)
            item_to_id = {i.item : i.id for i in checkin_result['items']}
            item_id_to_assembly_id = {i.product_id: i.assembly_id for i in checkin_result['assemblies']}
            for response in results:
                for result in response.result:
                    result.item_id = item_to_id[result.item]
                    result.assembly_id = item_id_to_assembly_id[result.item_id]
        return checkin_result

    async def log_failed_requests(self,
                                  plugin: Plugin,
                                  failed_requests: List[Tuple[ExecuteRequestUnion, Dict]]
                                  ):
        await asyncio.sleep(0)
        inputs = [{
            "plugin_id": plugin.id,
            "failure_reason": response_dict["failure_reason"],
            "failure_detail": response_dict["failure_detail"],
            "request": request.model_dump()

        } for (request, response_dict) in failed_requests]

        records = []
        if inputs:
            records = await upsert_execution_failures(self.db, inputs)
        return records 
