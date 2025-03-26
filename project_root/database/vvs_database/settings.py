import os 
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_STR: str = os.getenv('BACKEND_API_STR', '/api/v1')

    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: str = os.getenv("REDIS_PORT", "6379")
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")
    REDIS_DB: str = os.getenv('REDIS_DB', '0')
    REDIS_URL: str = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    REDIS_CACHE_TTL: int = int(os.getenv('REDIS_MESSAGE_TTL', 3600))

    POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_DB: str = os.getenv('POSTGRES_DB')
    POSTGRES_DB_TEST: str = os.getenv('POSTGRES_DB_TEST')

    SQLALCHEMY_DATABASE_URL: str = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB}"
    SQLALCHEMY_DATABASE_URL_SYNC: str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB}"
    DEFAULT_DB_URL: str = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/postgres"
    TEST_DB_URL: str = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB_TEST}"

    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "rabbitmq")
    RABBITMQ_PORT: int = int(os.environ.get('RABBITMQ_PORT', 5672))
    RABBITMQ_DEFAULT_USER: str = os.getenv('RABBITMQ_DEFAULT_USER')
    RABBITMQ_DEFAULT_PASS: str = os.getenv('RABBITMQ_DEFAULT_PASS')
    RABBITMQ_EXCHANGE_NAME: str = os.getenv('RABBITMQ_EXCHANGE_NAME')

    S3_ACCESS_KEY: str = os.getenv('S3_ACCESS_KEY')
    S3_SECRET_KEY: str = os.getenv('S3_SECRET_KEY')
    S3_BUCKET: str = os.getenv('S3_BUCKET')
    S3_UPLOAD_PREFIX: str = os.getenv('S3_UPLOAD_PREFIX')
    S3_SECURE_CONNECTION: bool = os.getenv('S3_SECURE_CONNECTION') == 'true'
    S3_URL: str = os.getenv('S3_URL')

settings = Settings()
