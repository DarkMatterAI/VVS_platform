
# from dagster import op, In, Out, Output, graph, GraphIn, GraphOut

# from .api_executor_ops import execute_api_plugin
# from .redis_ops import redis_get

# # @op(
# #         ins={'plugin_record': In(dict),
# #              'request_inputs': In(dict)},
# #         out={'result' : Out(dict)}
# # )
# # def plugin_executor(context, plugin_record, request_inputs):
# #     # TODO: hit cache
# #     # TODO: hit database


# #     if plugin_record['execution_type'].lower() == 'api':
# #         result = execute_api_plugin(context, plugin_record, request_inputs)
# #     else:
# #         result = {}

# #     # TODO: check output schema
# #     # TODO: log to database 

# #     context.log.info(f"Result {result}")

# #     return result 

# @op(
#         ins={'plugin_record' : In(dict)},
#         out={'api_executor' : Out(is_required=False),
#              'queue_executor' : Out(is_required=False),
#              }
# )
# def route_executor(plugin_record):
#     if plugin_record['execution_type'].lower() == 'api':
#         yield Output(plugin_record, 'api_executor')
#     else:
#         yield Output(plugin_record, 'queue_executor')

# @op(
#         ins={'plugin_record': In(dict),
#              'request_inputs': In(dict)},
#         out={'redis_key' : Out(str)}
# )
# def mock_message(context, plugin_record, request_inputs):
#     return ''

# @op(
#         ins={'redis_key' : In(str)},
#         out={'result' : Out(dict)}
# )
# def mock_poll(context, redis_key):
#     return {}


# @graph
# def execute_queue_plugin(plugin_records, request_input):
#     redis_key = mock_message(plugin_records, request_input)
#     result = mock_poll(redis_key)
#     return result 

# @op
# def merge(context, inputs):
#     result = inputs[0]
#     context.log.info(f"{result}")
#     return result 

# @graph
# def plugin_executor(plugin_record, request_inputs):
#     api_plugin, queue_plugin = route_executor(plugin_record)
#     api_result = execute_api_plugin(api_plugin, request_inputs)
#     queue_result = execute_queue_plugin(queue_plugin, request_inputs)
#     result = merge([api_result, queue_result])
#     return result 

