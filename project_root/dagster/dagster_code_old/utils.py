import uuid 

def get_request_key(group_key, plugin_type, plugin_id, item_id):
    request_id = uuid.uuid4()
    request_key = f"request.{group_key}.{plugin_type}.{plugin_id}.{item_id}.{request_id}"
    return request_key 

