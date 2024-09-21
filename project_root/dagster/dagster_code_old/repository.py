from dagster import repository
from .jobs.test_job_rabbitmq_redis import rabbitmq_redis_test_job, rabbitmq_redis_test_job_docker

@repository
def my_repository():
    return [
        rabbitmq_redis_test_job, rabbitmq_redis_test_job_docker,
    ]