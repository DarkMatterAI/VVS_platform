import os 
from dagster import job, op, Config, In, Out, Output, graph, config_mapping, DynamicOut, DynamicOutput
import random 


@op(out={"branch_1": Out(is_required=False), "branch_2": Out(is_required=False)})
def branching_op():
    num = random.randint(0, 1)
    if num == 0:
        yield Output(1, "branch_1")
    else:
        yield Output(2, "branch_2")


@op
def branch_1_op(context, input1):
    context.log.info(f"Executing branch 1")
    return input1


@op
def branch_2_op(context, input1):
    context.log.info(f"Executing branch 2")
    return input1


@op
def test_merge(context, input1):
    context.log.info(f"Merging {len(input1)} inputs")
    assert input1 == [1] or input1 == [2]
    return input1[0]

# @op(
#         ins={'result' : In(int)},
#         out={'result' : Out(int, is_required=False), 'fail' : Out(is_required=False)}
#     )
# def test_early_exit_op(context, result):
#     context.log.info(f"Result {result}")
#     if result < 2:
#         yield Output(result+1, 'result')
#     else:
#         yield Output(None, 'fail')

# class EarlyExitConfig(Config):
#     iters: int 

# @op(out={'n_iters' : Out(int)})
# def parse_config_mock(context, config: EarlyExitConfig):
#     return config.iters 

# @op(out=DynamicOut(int))
# def dynamic_range(context, config: EarlyExitConfig):    
#     for i in range(config.iters) :
#         yield DynamicOutput(value=i)

# @graph()
# def test_early_exit_graph(result):
#     n_iters = parse_config_mock()
#     for i in range(n_iters):
#         result, fail = test_early_exit_op(result)
#     return result 


@job
def test_job():
    branch_1, branch_2 = branching_op()
    r1 = branch_1_op(branch_1)
    r2 = branch_2_op(branch_2)
    result = test_merge([r1, r2])

    # n_iters = parse_config_mock()
    # result = test_early_exit_graph(result, n_iters)


