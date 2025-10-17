import os 
import httpx 
from app.plugin_data import mock_mapping

backend_url = f"http://backend:{os.environ['BACKEND_PORT']}"

def ping_backend():
    transport = httpx.HTTPTransport(retries=5)
    client = httpx.Client(transport=transport)
    response = client.get(f"{backend_url}/", timeout=5)
    assert response.status_code == 200, response.text 

def create_plugin(create_data):
    print(create_data)
    response = httpx.post(f"{backend_url}/api/v1/plugins/", json=create_data, timeout=5)
    assert response.status_code == 200, response.text 
    return response.json()

def get_current_records():
    target_count = {
        'embedding' : 3,
        'data_source' : 1,
        'filter' : 1,
        'score' : 1,
        'mapper' : 1,
        'assembly' : 1
    }

    records = {k:[] for k in target_count.keys()}

    response = httpx.get(f"{backend_url}/api/v1/plugins/",
                                params={'name' : 'mock_%_api_%'})
    assert response.status_code == 200, response.text 
    current_records = response.json()
    for record in current_records:
        plugin_type = record['type']
        target_count[plugin_type] -= 1
        if target_count[plugin_type] == 0:
            target_count.pop(plugin_type)
        records[plugin_type].append(record)
    print(f"Found {len(current_records)} existing records")
    return records, target_count 

def create_records():
    print('creating records')
    ping_backend()
    records, target_count = get_current_records()
    print(f"Creating records: " + ' '.join([f"{k} : {v}" for k,v in target_count.items()]))
    order = ['embedding', 'data_source', 'filter', 'score', 'mapper', 'assembly']

    for plugin_type in order:
        while target_count.get(plugin_type, 0) > 0:
            create_func = mock_mapping[plugin_type]

            if plugin_type == 'data_source':
                embedding_ids = [records['embedding'][0]]
                create_data = create_func(embedding_ids)
            elif plugin_type == 'mapper':
                input_embedding = records['embedding'][0]
                output_embeddings = records['embedding'][1:]
                create_data = create_func(input_embedding, output_embeddings)
            else:
                create_data = create_func()

            if plugin_type == 'filter':
                create_data["batch_size"] = 1 # to test single batch size

            plugin = create_plugin(create_data)
            records[plugin_type].append(plugin)
            target_count[plugin_type] -= 1

    print(records)

    return records 

def delete_records(records):
    print('deleting records')
    order = ['filter', 'score', 'assembly', 'mapper', 'data_source', 'embedding']
    for plugin_type in order:
        record_ids = records[plugin_type]
        for record_id in record_ids:
            response = httpx.delete(f"{backend_url}/api/v1/plugins/{record_id['id']}")
            assert response.status_code == 200, response.text 

    response = httpx.get(f"{backend_url}/api/v1/execute/item_cleanup")
    assert response.status_code == 200 


