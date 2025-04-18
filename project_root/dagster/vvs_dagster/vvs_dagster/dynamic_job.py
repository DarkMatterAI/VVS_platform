# import dagster as dg 
# import time 
# from typing import Tuple 

# class DynamicJobConfig(dg.Config):
#     item: str
#     iterations: int 

# @dg.op(out={"item": dg.Out(), "iterations": dg.Out()})
# def parse_config(context: dg.OpExecutionContext,
#                  config: DynamicJobConfig
#                  ) -> Tuple[str, int]:
#     return config.item, config.iterations 

# @dg.op(out=dg.DynamicOut())
# def serial_fan_out(context: dg.OpExecutionContext,
#                    iterations: int):
#     context.log.info("Fanning out iterations")
#     for iteration in range(iterations):
#         yield dg.DynamicOutput(iteration, mapping_key=str(iteration))

# # @dg.op(tags={"concurrency": "limited"})
# # def process_iteration(context: dg.OpExecutionContext,
# #                       iteration: int,
# #                       item: str):
# #     context.log.info(f"Starting iteration {iteration}")
# #     time.sleep(1.0)
# #     context.log.info(f"Finished iteration {iteration}")
# #     return True 

# @dg.op(tags={"concurrency": "limited", "dagster/priority": "0"})
# def process_iteration_1(context: dg.OpExecutionContext,
#                         iteration: int):
#     context.log.info(f"Starting iteration {iteration} op 1")
#     time.sleep(1.0)
#     context.log.info(f"Finished iteration {iteration} op 1")
#     return iteration 

# @dg.op(tags={"concurrency": "limited", "dagster/priority": "1"})
# def process_iteration_2(context: dg.OpExecutionContext,
#                         iteration: int,
#                         item: str):
#     context.log.info(f"Starting iteration {iteration} op 2")
#     time.sleep(1.0)
#     context.log.info(f"Finished iteration {iteration} op 2")
#     return True 

# @dg.graph
# def process_iteration(iteration: int,
#                       item: str):
#     iteration = process_iteration_1(iteration=iteration)
#     iteration = process_iteration_2(iteration=iteration, item=item)
#     return iteration 

# # @dg.op
# # def process_iteration_parallel(context: dg.OpExecutionContext,
# #                                iteration: int,
# #                                item: str):
# #     context.log.info(f"Starting iteration {iteration}")
# #     time.sleep(1.0)
# #     context.log.info(f"Finished iteration {iteration}")
# #     return True 

# @dg.op
# def collect(context: dg.OpExecutionContext,
#             results: list[bool]):
#     return True 


# default_config = dg.RunConfig(
#     ops={"parse_config": DynamicJobConfig(item='a', iterations=5)}
# )

# @dg.job(
#     config=default_config,
#     executor_def=dg.multiprocess_executor.configured({
#         "tag_concurrency_limits": [
#             {"key": "concurrency", "value": "limited", "limit": 1}
#         ]
#     })
# )
# def dynamic_job():
#     item, iterations = parse_config()
#     iterations = serial_fan_out(iterations=iterations)
#     results = iterations.map(lambda iteration: process_iteration(iteration=iteration, item=item))
#     collect(results.collect())

# # def dynamic_job():
# #     item, iterations = parse_config()
# #     iterations = serial_fan_out(iterations=iterations)
# #     results = iterations.map(lambda iteration: process_iteration(iteration=iteration, item=item))
# #     collect(results.collect())

#     # results2 = iterations.map(lambda iteration: process_iteration_parallel(iteration=iteration, item=item))
#     # collect(results2.collect())

