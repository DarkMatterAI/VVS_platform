import os 
from fastapi import UploadFile
from aioredis import Redis
from minio import Minio
from vvs_database.core import get_engine, get_session_factory, create_all_tables
from vvs_database import settings 

engine = get_engine(settings.SQLALCHEMY_DATABASE_URL)
AsyncSessionLocal = get_session_factory(engine)
s3_client = Minio(settings.S3_URL,
                  access_key=settings.S3_ACCESS_KEY,
                  secret_key=settings.S3_SECRET_KEY,
                  secure=settings.S3_SECURE_CONNECTION)

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
        s3_client._http.clear()

def upload_file(file: UploadFile, s3_client):
    print('uploading file')
    result = s3_client.put_object(bucket_name=settings.S3_BUCKET,
                                  object_name=file.filename,
                                  data=file.file,
                                  length=os.fstat(file.file.fileno()).st_size)
    print(result)
    return result 

async def init_db():
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    lock = redis.lock("db_init_lock", timeout=60)

    try:
        print('Acquiring redis lock for database init')
        await lock.acquire()
        await create_all_tables(engine)

        bucket_found = s3_client.bucket_exists(settings.S3_BUCKET)
        if not bucket_found:
            print("Creating S3 bucket")
            s3_client.make_bucket(settings.S3_BUCKET)
    finally:
        print('Releasing redis lock')
        await lock.release()
        await redis.close()
        s3_client._http.clear()

