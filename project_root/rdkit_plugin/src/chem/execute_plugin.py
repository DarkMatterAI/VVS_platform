from pydantic import BaseModel 
from typing import Union, List, Optional 
from datetime import datetime

from .property_filter import property_filter
from .reaction_assembly import reaction_assembly
from .synton_assembly import synton_assembly

from ..utils import date_print
from ..connections import get_plugin_from_routing_key

class NamedEmbedding(BaseModel):
    id: int # internal id
    name: str
    embedding: List[float]

class ItemRequest(BaseModel):
    request_id: str 
    id: Union[int, str]
    external_id: Union[int, str]
    item: str 
    embedding: Optional[List[NamedEmbedding]]=None
        
class FilterResponse(BaseModel):
    valid: bool

class AssemblyItem(BaseModel):
    assembly_index: int 
    id: Union[int, str]
    external_id: Union[int, str]
    item: str 

class AssemblyRequest(BaseModel):
    request_id: str 
    parents: List[AssemblyItem]

class AssemblyResult(BaseModel):
    item: str 
    external_id: Optional[Union[int, str]]
        
class AssemblyResponse(BaseModel):
    valid: bool
    result: List[AssemblyResult]

class_map = {
    'property_filter' : {'schema' : ItemRequest, 'func' : property_filter},
    'smarts_assembly' : {'schema' : AssemblyRequest, 'func' : reaction_assembly},
    'synton_assembly' : {'schema' : AssemblyRequest, 'func' : synton_assembly},
}

def classify_plugin(plugin_record):
    if plugin_record['group_key'] != 'rdkit_plugin':
        return '', False 
    elif plugin_record['type'].lower() == 'filter':
        return 'property_filter', True 
    elif ((plugin_record['type'].lower() == 'assembly') and 
          plugin_record['config'].keys() == set(['single_stage_reactions', 'multi_stage_reactions'])):
        return 'smarts_assembly', True 
    elif ((plugin_record['type'].lower() == 'assembly') and 
          plugin_record['config'].keys() == set(['synt_on_reaction_stages'])):
        return 'synton_assembly', True 
    else:
        return '', False 
    
def validate_message(message, plugin_class):
    try:
        _ = class_map[plugin_class]['schema'](**message)
        return True 
    except:
        return False 

def execute_plugin(engine, message_data, routing_key):
    plugin_record, plugin_id = get_plugin_from_routing_key(engine, routing_key)

    date_print('Classifying plugin')
    plugin_class, valid = classify_plugin(plugin_record)
    if not valid: 
        return {}, False, f'Could not classify record {plugin_record}'
    
    date_print('Validating message')
    if not validate_message(message_data, plugin_class):
        return {}, False, f'Invalid message format {message_data}'
    
    date_print('Executing plugin')
    result = class_map[plugin_class]['func'](plugin_record, message_data)
    return result 
