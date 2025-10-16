from pydantic import BaseModel 

from vvs_database.settings import settings 

class RabbitMQConnection(BaseModel):
    host: str=settings.RABBITMQ_HOST
    port: str=settings.RABBITMQ_PORT
    username: str=settings.RABBITMQ_DEFAULT_USER
    password: str=settings.RABBITMQ_DEFAULT_PASS
    exchange: str=settings.RABBITMQ_EXCHANGE_NAME

class RedisConnection(BaseModel):
    redis_url: str=settings.REDIS_URL
    cache_ttl: int=settings.REDIS_CACHE_TTL

class PostgresConnection(BaseModel):
    postgres_url: str=settings.SQLALCHEMY_DATABASE_URL


