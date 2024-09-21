import os
from dagster import resource, ConfigurableResource, InitResourceContext
from pydantic import Field
from sqlalchemy import create_engine

class PostgresResourceConfig(ConfigurableResource):
    host: str = Field(default="postgresql")
    db_name: str = Field(default=os.environ.get('POSTGRES_DB', ''))
    username: str = Field(default=os.environ.get('POSTGRES_USER', ''))
    password: str = Field(default=os.environ.get('POSTGRES_PASSWORD', ''))

@resource(config_schema=PostgresResourceConfig.to_config_schema())
def postgres_resource(context: InitResourceContext):
    config = PostgresResourceConfig.from_resource_context(context)
    db_url = f"postgresql://{config.username}:{config.password}@{config.host}/{config.db_name}"
    engine = create_engine(db_url)
    try:
        yield engine
    finally:
        engine.dispose()

# import os 
# from dagster import resource, Config, Field, String
# from sqlalchemy import create_engine

# class PostgresResourceConfig(Config):
#     host: str = Field(default_value="postgresql")
#     db_name: str = Field(default_value=os.environ.get('POSTGRES_DB', ''))
#     username: str = Field(default_value=os.environ.get('POSTGRES_USER', ''))
#     password: str = Field(default_value=os.environ.get('POSTGRES_PASSWORD', ''))

# class PostgresResourceConfig(Config):
#     host: str = Field(String, default_value="postgresql")
#     db_name: str = Field(String, default_value=os.environ.get('POSTGRES_DB', ''))
#     username: str = Field(String, default_value=os.environ.get('POSTGRES_USER', ''))
#     password: str = Field(String, default_value=os.environ.get('POSTGRES_PASSWORD', ''))

# @resource(config_schema=PostgresResourceConfig)
# def postgres_resource(init_context):
#     config = init_context.resource_config
#     db_url = f"postgresql://{config.username}:{config.password}@{config.host}/{config.db_name}"
#     engine = create_engine(db_url)
#     try:
#         yield engine 
#     finally:
#         engine.dispose()