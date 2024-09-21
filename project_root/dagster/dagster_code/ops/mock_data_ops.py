# import numpy as np 
# import uuid 
# import string 

# from dagster import op, In, Out

# def get_request_id(plugin_record, item):
#     k1 = plugin_record['group_key']
#     k2 = plugin_record['type']
#     k3 = plugin_record['id']
#     k4 = item.get('id', np.random.randint(1e5))
#     k5 = uuid.uuid4() # request id

#     request_id = f"request.{k1}.{k2}.{k3}.{k4}.{k5}"
#     return request_id 

# def get_random_item(plugin_record):
#     random_item = {
#         'request_id' : None,
#         'id' : np.random.randint(1e5),
#         'external_id' : str(uuid.uuid4()),
#         'item' : ''.join(np.random.choice([i for i in string.ascii_lowercase], 16)),
#     }
#     embedding_records = plugin_record['embedding_records']
#     if embedding_records:
#         random_item['embedding'] = []
#         for embedding_record in embedding_records:
#             named_embedding = {
#                 'id' : embedding_record['id'],
#                 'name' : embedding_record['name'],
#                 'embedding' : np.random.randn(embedding_record['vector_length']).tolist()
#             }
#             random_item['embedding'].append(named_embedding)
    
#     random_item['request_id'] = get_request_id(plugin_record, random_item)
#     return random_item 

# def get_mock_input_item(plugin_record):
#     return get_random_item(plugin_record)

# def get_mock_data_source_input(plugin_record):
#     input = get_random_item(plugin_record)
#     input.pop('id')
#     input.pop('external_id')
#     input.pop('external_id')
#     input['k'] = 5
#     return input 

# def get_mock_mapper_input(plugin_record):
#     input_embedding_id = plugin_record['input_embedding_id']
#     input_embedding_record = [i for i in plugin_record['embedding_records']
#                               if i['id'] == input_embedding_id][0]
#     input = {
#         'request_id' : get_request_id(plugin_record, {}),
#         'embedding' : {
#             'id' : input_embedding_id,
#             'name' : input_embedding_record['name'],
#             'embedding' : np.random.randn(input_embedding_record['vector_length']).tolist()
#         }
#     }
#     return input 

# def get_mock_assembly_input(plugin_record):
#     parents = []
#     for i in range(plugin_record['num_parents']):
#         parent_item = get_random_item(plugin_record)
#         parent_item.pop('request_id')
#         parents.append(parent_item)

#     input = {
#         'request_id' : get_request_id(plugin_record, {}),
#         'parents' : parents
#     }
#     return input 

# type_to_request_func = {
#     'embedding' : get_mock_input_item,
#     'data_source' : get_mock_data_source_input,
#     'filter' : get_mock_input_item,
#     'score' : get_mock_input_item,
#     'mapper' : get_mock_mapper_input,
#     'assembly' : get_mock_assembly_input
# }


# @op(
#         ins={'plugin_record' : In(dict)},
#         out={'mock_data' : Out(dict)}
# )
# def generate_mock_data(context, plugin_record: dict):
#     context.log.info(f"Generating mock data for plugin {plugin_record['id']}")
#     mock_data = type_to_request_func[plugin_record['type'].lower()](plugin_record)
#     context.log.info(f"{mock_data}")
#     return mock_data
