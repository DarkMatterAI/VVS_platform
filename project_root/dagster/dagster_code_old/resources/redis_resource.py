import os
from dagster import resource, ConfigurableResource, InitResourceContext
from pydantic import Field
import redis

class RedisResourceConfig(ConfigurableResource):
    host: str = Field(default="redis")
    port: int = Field(default=int(os.environ.get('REDIS_PORT', 6379)))
    password: str = Field(default=os.environ.get('REDIS_PASSWORD', ''))
    db: int = Field(default=int(os.environ.get('REDIS_DB', 0)))

@resource(config_schema=RedisResourceConfig.to_config_schema())
def redis_resource(context: InitResourceContext):
    config = RedisResourceConfig.from_resource_context(context)
    redis_client = redis.Redis(
        host=config.host,
        port=config.port,
        password=config.password,
        db=config.db
    )
    try:
        yield redis_client
    finally:
        redis_client.close()

