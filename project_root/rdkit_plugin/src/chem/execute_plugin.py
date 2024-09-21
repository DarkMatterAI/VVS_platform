from pydantic import BaseModel 
from typing import Union, List, Optional 
from datetime import datetime

from .property_filter import property_filter
from ..utils import date_print

from ..connections import get_plugin_from_routing_key

class NamedEmbedding(BaseModel):
    id: int # internal id
    name: str
    embedding: List[float]
    gradient: Optional[List[float]]=None

class ItemRequest(BaseModel):
    request_id: str 
    id: Union[int, str]
    external_id: Union[int, str]
    item: str 
    embedding: Optional[List[NamedEmbedding]]=None
        
class FilterResponse(BaseModel):
    valid: bool

type_map = {
    'filter' : {'schema' : ItemRequest, 'func' : property_filter}
}


def validate_message(message, plugin_type):
    try:
        type_map[plugin_type]['schema'].model_validate(message)
        return True 
    except:
        return False 
    
def plugin_function(message_data, plugin_record, plugin_type):
    result = type_map[plugin_type]['func'](plugin_record, message_data)
    date_print(f"{plugin_record['id']}, {message_data.get('item', '')}, {result[0]}")
    return result 


def execute_plugin(engine, message_data, routing_key):
    plugin_record, plugin_id = get_plugin_from_routing_key(engine, routing_key)
    plugin_type = plugin_record['type'].lower()

    if ((plugin_type not in type_map.keys()) or 
        (not validate_message(message_data, plugin_type))):
        return {}, False, f'Invalid message format: {message_data}'
    
    return plugin_function(message_data, plugin_record, plugin_type)

    # return type_map[plugin_type]['func'](plugin_record, message_data)


