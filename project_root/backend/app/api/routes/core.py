from fastapi import APIRouter, Depends, File, UploadFile
from app.core.database import get_s3_client, upload_file

import time 

router = APIRouter()

@router.get("/")
async def read_root():
    return {"Hello": "World"}

@router.get("/slow_test")
async def slow_test():
    time.sleep(1)
    return {"Hello": "World"}

# @router.post("/upload")
# async def upload_file(file: UploadFile,
#                       s3_client=Depends(get_s3_client)):
#     result = s3_client.put_object(
#         "files",
#         "object",
#         data=file,
#         content_type=file.content_type
#     )

@router.post("/upload")
def upload_file_endpoint(file: UploadFile,
                         s3_client=Depends(get_s3_client)):
    print('upload route')
    result = upload_file(file, s3_client)
    return result 


    # file_content = file.file
    # file_content.seek(0, os.SEEK_END)
    # file_size = file_content.tell()
    # file_content.seek(0)
    # result = minio_client.put_object(
    #         "files",
    #         "object",
    #         data=file_content,
    #         content_type=file.content_type,
    #         length=file_size
    #     )