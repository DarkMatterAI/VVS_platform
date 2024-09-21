
import os 
from dagster import Definitions, EnvVar, FilesystemIOManager

from .jobs.test_job import test_job
from .jobs.job_search_iteration import parse_search_config_job

from .resources import PostgresResourceConfig, RedisResourceConfig

resources_by_env = {
    "PROD": {
        # "io_manager": S3PickleIOManager(s3_resource=S3Resource(), s3_bucket="my-bucket")
    },
    "LOCAL": {
        "io_manager": FilesystemIOManager(),
        "postgres" : PostgresResourceConfig(host="postgresql",
                                            db_name=EnvVar("DAGSTER_POSTGRES_DB"),
                                            username=EnvVar("DAGSTER_POSTGRES_USER"),
                                            password=EnvVar("DAGSTER_POSTGRES_PASSWORD")),
        "redis" : RedisResourceConfig(host=EnvVar('REDIS_HOST'),
                                      port=EnvVar.int('REDIS_PORT'),
                                      password=EnvVar("REDIS_PASSWORD"),
                                      db=EnvVar.int("REDIS_DB")),
    },
}

defs = Definitions(
    jobs=[
        test_job,
        parse_search_config_job
        ],
    resources=resources_by_env[os.getenv('DEPLOY', 'LOCAL')]
)
