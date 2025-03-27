from fastapi import APIRouter

import time 

router = APIRouter()

@router.get("/")
async def read_root():
    return {"Hello": "World"}

@router.get("/slow_test")
async def slow_test():
    time.sleep(1)
    return {"Hello": "World"}
