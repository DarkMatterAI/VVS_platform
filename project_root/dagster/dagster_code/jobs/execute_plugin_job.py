# import os 
# from dagster import job, op, Config

# from ..ops.postgres_ops import get_plugin_from_op
# from ..ops.mock_data_ops import generate_mock_data
# from ..ops.plugin_executor import plugin_executor

# class ExecutePluginMock(Config):
#     plugin_id: int 

# @op
# def parse_config_mock(context, config: ExecutePluginMock):
#     return config.plugin_id 

# @job
# def execute_plugin_mock_job():
#     plugin_id = parse_config_mock()
#     plugin_record = get_plugin_from_op(plugin_id)
#     mock_data = generate_mock_data(plugin_record)
#     result = plugin_executor(plugin_record, mock_data)
    

