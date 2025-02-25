from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

def get_engine(connection_string):
    return create_async_engine(connection_string, echo=False)

def get_session_factory(engine):
    return sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def create_all_tables(engine):
    """Create all database tables defined in the models."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)