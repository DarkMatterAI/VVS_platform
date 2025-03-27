from fastapi import APIRouter, Depends, UploadFile
from app.core.database import get_s3_client
from app.crud import upload_file, delete_file

router = APIRouter()

@router.post("/upload")
def upload_file_endpoint(file: UploadFile,
                         s3_client=Depends(get_s3_client)):
    result = upload_file(file.filename, file.file, s3_client)
    return result 

@router.delete("/{filename}")
def delete_file_endpoint(filename: str,
                         s3_client=Depends(get_s3_client)):
    result = delete_file(filename, s3_client)
    return result 
    
