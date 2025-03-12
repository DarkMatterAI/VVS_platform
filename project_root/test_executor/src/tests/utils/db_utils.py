from vvs_database import crud, schemas 
from vvs_database.utils import plugin_type_map

def dict_to_model(data, plugin, model_key):
    if type(data) != list:
        data = [data]

    if type(data[0]) == dict:
        model = plugin_type_map[plugin['type']][model_key]
        data = [model.model_validate(i) for i in data]
    return data 

def request_dict_to_model(request_data, plugin):
    return dict_to_model(request_data, plugin, 'execute_request_model')

def response_dict_to_model(response_data, plugin):
    return dict_to_model(response_data, plugin, 'execute_response_model')

def validate_execution_cache(redis_connection, request_data, plugin):
    request_data = request_dict_to_model(request_data, plugin)
    for request in request_data:
        cache_key = request.generate_key(plugin['id'])
        cache_response = redis_connection.get(cache_key)
        assert cache_response is not None

async def validate_item_checkin(db_session, request_data, response_data, plugin, db_persist):
    request_data = request_dict_to_model(request_data, plugin)
    response_data = response_dict_to_model(response_data, plugin)
    for request, response in zip(request_data, response_data):
        result = await crud.get_item_result(db_session, request.item_data.item_id, plugin['id'])
        if db_persist or plugin['type']=='score':
            assert result is not None 
            assert result.valid == response.valid, (result, response)
            assert result.score == getattr(response, 'score', None)
            assert result.embedding == getattr(response, 'embedding', None)
        else:
            assert result is None 

async def validate_data_source_checkin(db_session, response_data, plugin, db_persist):
    response_data = response_dict_to_model(response_data, plugin)
    for result in response_data:
        assert result.valid 
        assert len(result.result) > 0
        for item in result.result:
            assert item.item_id is not None 
            item_source = await crud.get_item_source(db_session, item.item_id, plugin['id'])
            assert item_source is not None 

            if db_persist:
                embedding_id = plugin['embedding_ids'][0]
                item_result = await crud.get_item_result(db_session, item.item_id, embedding_id)
                assert item_result is not None 

async def validate_assembly_checkin(db_session, request_data, response_data, plugin):
    request_data = request_dict_to_model(request_data, plugin)
    response_data = response_dict_to_model(response_data, plugin)
    for request, response in zip(request_data, response_data):
        assert response.valid 
        assert len(response.result)>0

        for assembly_result in response.result:
            assert assembly_result.item_id is not None 
            item_source = await crud.get_item_source(db_session, assembly_result.item_id, plugin['id'])
            assert item_source is not None, assembly_result

            assembly_record = await crud.get_assembly_by_product_plugin(db_session, 
                                                                        assembly_result.item_id, 
                                                                        plugin['id'])
            assert assembly_record is not None 
            request_parents = sorted(request.parents, key=lambda x: x.assembly_index)
            record_parents = sorted(assembly_record.components, key=lambda x: x.assembly_index)

            for i in range(len(request_parents)):
                assert request_parents[i].item_id == record_parents[i].component_id

