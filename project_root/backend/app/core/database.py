import os 
from fastapi import UploadFile
from aioredis import Redis
from minio import Minio
import boto3 
from botocore.exceptions import ClientError
from vvs_database.core import get_engine, get_session_factory, create_all_tables
from vvs_database import settings, logging
from app import utils

engine = get_engine(settings.SQLALCHEMY_DATABASE_URL)
AsyncSessionLocal = get_session_factory(engine)
s3_client = boto3.client(
    's3',
    endpoint_url=settings.S3_URL,
    aws_access_key_id=settings.S3_ACCESS_KEY,
    aws_secret_access_key=settings.S3_SECRET_KEY,
    aws_session_token=settings.S3_SESSION_TOKEN,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=settings.S3_VERIFY_SSL
)
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

def upload_file(file: UploadFile, s3_client):
    logging.info('uploading file')
    object_name = f"{settings.S3_UPLOAD_PREFIX}/{file.filename}"
    result = s3_client.put_object(
        Bucket=settings.S3_BUCKET,
        Key=object_name,
        Body=file.file,
        # ContentLength=os.fstat(file.file.fileno()).st_size
    )
    logging.info(result)
    return result

async def init_db():
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    lock = redis.lock("db_init_lock", timeout=60)

    try:
        logging.info('Acquiring redis lock for database init')
        await lock.acquire()
        await create_all_tables(engine)

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=settings.S3_BUCKET)
            bucket_found = True
        except ClientError:
            bucket_found = False

        if not bucket_found:
            logging.info("Creating S3 bucket")
            if settings.S3_REGION:
                s3_client.create_bucket(
                    Bucket=settings.S3_BUCKET,
                    CreateBucketConfiguration={'LocationConstraint': settings.S3_REGION}
                )
            else:
                s3_client.create_bucket(Bucket=settings.S3_BUCKET)
    finally:
        logging.info('Releasing redis lock')
        await lock.release()
        await redis.close()

