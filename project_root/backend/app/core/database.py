from aioredis import Redis
from vvs_database.core import get_engine, get_session_factory, create_all_tables
# import os
from app.core.settings import settings 

# REDIS_HOST = os.getenv("REDIS_HOST", "redis")
# REDIS_PORT = os.getenv("REDIS_PORT", "6379")
# REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
# REDIS_DB = os.getenv('REDIS_DB', '0')
# REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# POSTGRES_USER = os.getenv('POSTGRES_USER')
# POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
# POSTGRES_DB = os.getenv('POSTGRES_DB')

# SQLALCHEMY_DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB}"

engine = get_engine(settings.SQLALCHEMY_DATABASE_URL)
AsyncSessionLocal = get_session_factory(engine)

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

async def init_db():
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    lock = redis.lock("db_init_lock", timeout=60)

    try:
        print('Acquiring redis lock for database init')
        await lock.acquire()
        await create_all_tables(engine)
    finally:
        print('Releasing redis lock')
        await lock.release()
        await redis.close()
