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

@router.post("/upload")
def upload_file_endpoint(file: UploadFile,
                         s3_client=Depends(get_s3_client)):
    result = upload_file(file, s3_client)
    return result 
