import os 

from vvs_database.crud.s3_crud import (
    get_s3_client, 
    check_file_exists,
)

test_files = '/code/test_files'

def test_upload_delete(backend_client):
    s3_client = get_s3_client()
    filename = f"{test_files}/zinc_10.csv"
    assert os.path.exists(filename)

    upload_name = 'test_backend_upload_delete.csv'
    with open(filename, "rb") as f:
        file_content = f.read()

    files = {"file": (upload_name, file_content)}
    response = backend_client.post('/api/v1/files/upload', files=files)
    response.raise_for_status()
    assert check_file_exists(upload_name, s3_client)

    response = backend_client.delete(f'/api/v1/files/{upload_name}')
    response.raise_for_status()
    assert not check_file_exists(upload_name, s3_client)

