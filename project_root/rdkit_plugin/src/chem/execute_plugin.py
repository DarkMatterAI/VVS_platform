from ..connections import get_plugin_from_routing_key

def execute_plugin(engine, message_data, routing_key):
    # return {}, False, f"Plugin not found"
    print(message_data)
    plugin_record, plugin_id = get_plugin_from_routing_key(engine, routing_key)

    if (not plugin_record) or (int(plugin_id)==2):
        print('Failed to find record')
        return {}, False, f"Plugin {plugin_id} not found"
    
    return {}, True, None 


