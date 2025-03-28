import boto3 
from botocore.exceptions import ClientError
from vvs_database import logging, settings 

def add_s3_prefix(filename):
    if not filename.startswith(settings.S3_UPLOAD_PREFIX):
        filename = f"{settings.S3_UPLOAD_PREFIX}/{filename}"
    return filename 

def get_s3_client():
    s3_client = boto3.client(
        's3',
        endpoint_url=settings.S3_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        aws_session_token=settings.S3_SESSION_TOKEN,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=settings.S3_VERIFY_SSL
    )
    return s3_client 

def check_file_exists(filename: str, s3_client=None):
    if s3_client is None:
        s3_client = get_s3_client()

    filename = add_s3_prefix(filename)
        
    try:
        s3_client.head_object(Bucket=settings.S3_BUCKET, 
                              Key=filename)
        return True
    except ClientError:
        return False
    
def get_file(filename: str, s3_client=None):
    logging.info(f"Getting object {filename}")
    filename = add_s3_prefix(filename)
    response = s3_client.get_object(Bucket=settings.S3_BUCKET, 
                                    Key=filename)
    return response 

def check_bucket_exists(bucket_name: str, s3_client=None):
    if s3_client is None:
        s3_client = get_s3_client()

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        bucket_found = True
    except ClientError:
        bucket_found = False
    
    return bucket_found 

def init_bucket(s3_client):
    bucket_found = check_bucket_exists(settings.S3_BUCKET, s3_client)
    if not bucket_found:
        logging.info("S3 bucket not found, creating")
        if settings.S3_REGION:
            s3_client.create_bucket(
                Bucket=settings.S3_BUCKET,
                CreateBucketConfiguration={'LocationConstraint': settings.S3_REGION}
            )
        else:
            s3_client.create_bucket(Bucket=settings.S3_BUCKET)

    
def upload_file(filename, body, s3_client):
    logging.info(f"Uploading file {filename}")
    object_name = add_s3_prefix(filename)
    result = s3_client.put_object(Bucket=settings.S3_BUCKET,
                                  Key=object_name,
                                  Body=body)
    logging.info(result)
    return result 

def delete_file(filename, s3_client):
    logging.info(f"Deleting file {filename}")
    object_name = add_s3_prefix(filename)
    result = s3_client.delete_object(Bucket=settings.S3_BUCKET,
                                     Key=object_name)
    return result 

