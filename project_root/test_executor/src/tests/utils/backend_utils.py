
def backend_get_plugins_by_filter(backend_client, 
                                  name_pattern: str=None, 
                                  group_key: str=None, 
                                  plugin_type: str=None,
                                  plugin_class: str=None
                                  ):
    params = {
        'name' : name_pattern,
        'group_key' : group_key,
        'plugin_type' : plugin_type,
        'plugin_class' : plugin_class 
    }
    params = {k:v for k,v in params.items() if v is not None}
    response = backend_client.get("/api/v1/plugins/", params=params)
    response.raise_for_status()
    return response.json()

def backend_execute_plugin(backend_client, request_data, plugin_id, params=None):
    endpoint = f"/api/v1/execute/{plugin_id}"

    default_params = {"cache": False,
                      "db_lookup": False,
                      "db_persist": False,
                      "use_semaphore": False,
                      "max_semaphore_attempts": 20,
                      "queue_polling_interval": 0.2,
                      "backoff_factor": 2.0
                      }
    if params is not None:
        default_params.update(params)

    if type(request_data) == list and len(request_data)==1:
        request_data = request_data[0]

    request_data = {
        "execute_request": request_data,
        "execute_params": default_params
    }
    response = backend_client.post(endpoint, json=request_data, timeout=30)
    return response 

def backend_delete_plugin(backend_client, endpoint, plugin_record, ignore_404=False):
    endpoint = f"{endpoint}/{plugin_record['id']}"
    response = backend_client.delete(endpoint)
    if ignore_404:
        assert response.status_code in [200, 404], (response.status_code, response.text)
    else:
        assert response.status_code == 200, (response.status_code, response.text)

