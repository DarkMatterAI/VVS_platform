from aioredis import Redis
from vvs_database.core import get_engine, get_session_factory, create_all_tables
# from vvs_database import settings, logging
from vvs_database import logging
from vvs_database.settings import settings 
from vvs_database.crud import get_s3_client, init_bucket
from app import utils

engine = get_engine(settings.SQLALCHEMY_DATABASE_URL)
AsyncSessionLocal = get_session_factory(engine)
s3_client = get_s3_client()
launch_config = utils.read_config()

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

def get_s3_client():
    # TODO: better connection handling
    try:
        yield s3_client 
    finally:
        s3_client.close()

async def get_redis_client():
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    try:
        yield redis 
    finally:
        await redis.close()

async def init_db():
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    lock = redis.lock("db_init_lock", timeout=60)

    try:
        logging.info('Acquiring redis lock for database init')
        await lock.acquire()
        await create_all_tables(engine)
        init_bucket(s3_client)

    finally:
        logging.info('Releasing redis lock')
        await lock.release()
        await redis.close()

