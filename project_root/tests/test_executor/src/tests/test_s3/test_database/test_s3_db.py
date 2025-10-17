import os 

from vvs_database.crud.s3_crud import (
    get_s3_client, 
    check_bucket_exists,
    check_file_exists,
    upload_file,
    delete_file
)
from vvs_database.settings import settings 

test_files = '/code/test_files'

def test_bucket_created():
    s3_client = get_s3_client()
    bucket_found = check_bucket_exists(settings.S3_BUCKET, s3_client)
    assert bucket_found 

def test_upload_delete():
    s3_client = get_s3_client()
    filename = f"{test_files}/zinc_10.csv"
    assert os.path.exists(filename)

    upload_name = 'test_db_upload_delete.csv'
    with open(filename, 'rb') as file_obj:
        result = upload_file(upload_name, file_obj, s3_client)

    assert check_file_exists(upload_name, s3_client)
    delete_file(upload_name, s3_client)
    assert not check_file_exists(upload_name, s3_client)
