from aioredis import Redis
from vvs_database.core import get_engine, get_session_factory, create_all_tables
from app.core.settings import settings 

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
