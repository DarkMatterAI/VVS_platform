import dagster as dg
from dagster_aws.s3 import S3Resource, S3PickleIOManager

# from typing import Optional

from vvs_database.core import get_engine, get_session_factory
from vvs_database.execution.connections.database import DatabaseService
# from vvs_database.schemas import ExecutePlugin, ExecuteParams

class PostgresResource(dg.ConfigurableResource):
    postgres_user: str 
    postgres_password: str 
    postgres_db: str 

    def get_db(self):
        database_url = f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@postgresql/{self.postgres_db}"
        engine = get_engine(database_url)
        AsyncSession = get_session_factory(engine)
        session = AsyncSession()
        return session 

    def get_db_service(self):
        session = self.get_db()
        db_service = DatabaseService(session)
        return db_service
    
S3 = S3Resource(aws_access_key_id=dg.EnvVar('S3_ACCESS_KEY'),
                aws_secret_access_key=dg.EnvVar('S3_SECRET_KEY'),
                endpoint_url=dg.EnvVar('S3_URL_DAGSTER'))
    

RESOURCE_DEFAULTS = {
    "postgres_resource": PostgresResource(postgres_user=dg.EnvVar("POSTGRES_USER"),
                                          postgres_password=dg.EnvVar("POSTGRES_PASSWORD"),
                                          postgres_db=dg.EnvVar("POSTGRES_DB")),
    "s3_resource": S3,
    "io_manager": S3PickleIOManager(s3_resource=S3,
                                    s3_bucket=dg.EnvVar('S3_BUCKET')),
}

