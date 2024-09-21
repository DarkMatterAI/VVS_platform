from fastapi import FastAPI

from app.core.database import init_db
from app.core.settings import settings 
from app.api.main import api_router

app = FastAPI(
    title='VVS',
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
)
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.get("/")
async def read_root():
    return {"Hello": "World"}

