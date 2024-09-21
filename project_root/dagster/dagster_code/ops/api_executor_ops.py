# import httpx 

# from dagster import op, In, Out

# def post_request(url, timeout, retries, inputs):
#     # TODO: logging
#     transport = httpx.HTTPTransport(retries=retries)
#     with httpx.Client(transport=transport) as client:
#         response = client.post(url, json=inputs, timeout=timeout)

#     assert response.status_code == 200
#     result = response.json()
#     return result 

# # # @op(
# # #         ins={'plugin_record': In(dict),
# # #              'request_inputs': In(dict)},
# # #         out={'result' : Out(dict)}
# # # )
# # def execute_api_plugin(context, plugin_record, request_inputs):
# #     context.log.info(f"Sending Request for plugin {plugin_record['id']}")
# #     result = post_request(plugin_record['endpoint_url'], 
# #                           plugin_record['timeout'], 
# #                           plugin_record['max_retries'], 
# #                           request_inputs)
# #     return result 


# @op(
#         ins={'plugin_record': In(dict),
#              'request_inputs': In(dict)},
#         out={'result' : Out(dict)}
# )
# def execute_api_plugin(context, plugin_record, request_inputs):
#     context.log.info(f"Sending Request for plugin {plugin_record['id']}")
#     result = post_request(plugin_record['endpoint_url'], 
#                           plugin_record['timeout'], 
#                           plugin_record['max_retries'], 
#                           request_inputs)
#     return result 



