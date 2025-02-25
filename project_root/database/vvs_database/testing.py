from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def terminate_database_connections(engine, db_name):
    """Terminate all connections to a database"""
    async with engine.connect() as conn:
        await conn.execute(
            text(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}'")
        )

async def create_test_database_url(default_db_url, test_db_name):
    """Create a test database and return its URL"""
    engine = create_async_engine(
        default_db_url,
        isolation_level="AUTOCOMMIT",
    )
    
    try:
        async with engine.connect() as conn:
            # Terminate connections and recreate database
            await conn.execute(
                text(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{test_db_name}'")
            )
            await conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_name}"))
            await conn.execute(text(f"CREATE DATABASE {test_db_name}"))
            
        # Construct test DB URL
        parts = default_db_url.split('/')
        test_db_url = '/'.join(parts[:-1] + [test_db_name])
        return test_db_url
    finally:
        await engine.dispose()

async def drop_test_database(default_db_url, test_db_name):
    """Drop the test database"""
    engine = create_async_engine(
        default_db_url,
        isolation_level="AUTOCOMMIT",
    )
    
    try:
        async with engine.connect() as conn:
            await conn.execute(
                text(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{test_db_name}'")
            )
            await conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_name}"))
    finally:
        await engine.dispose()