from fastapi import FastAPI

from app.core.database import init_db
from app.core.init_records import init_records
from vvs_database import logging
from vvs_database.settings import settings  
from app.api.main import api_router

app = FastAPI(
    title='VVS',
    openapi_url=f"{settings.API_STR}/openapi.json",
)
app.include_router(api_router, prefix=settings.API_STR)

@app.on_event("startup")
async def startup_event():
    logger = logging._init_default_logger('vvs_backend')
    logging.set_logger(logger)
    await init_db()
    await init_records()

@app.get("/")
async def read_root():
    return {"Hello": "World", "service" : "backend"}

