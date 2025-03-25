import dagster as dg

from vvs_database.core import get_engine, get_session_factory
from vvs_database.execution.connections.database import DatabaseService

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

RESOURCE_DEFAULTS = {
    "postgres_resource": PostgresResource(postgres_user=dg.EnvVar("POSTGRES_USER"),
                                          postgres_password=dg.EnvVar("POSTGRES_PASSWORD"),
                                          postgres_db=dg.EnvVar("POSTGRES_DB"))
}



    # import dagster as dg
    # import requests

    # from requests import Response

    # class MyConnectionResource(dg.ConfigurableResource):
    #     username: str

    #     def request(self, endpoint: str) -> Response:
    #         return requests.get(
    #             f"https://my-api.com/{endpoint}",
    #             headers={"user-agent": "dagster"},
    #         )

    # @dg.asset
    # def data_from_service(my_conn: MyConnectionResource) -> dict[str, Any]:
    #     return my_conn.request("/fetch_data").json()

    # defs = dg.Definitions(
    #     assets=[data_from_service],
    #     resources={
    #         "my_conn": MyConnectionResource(username="my_user"),
    #     },
    # )


    # POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    # POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')
    # POSTGRES_DB: str = os.getenv('POSTGRES_DB')
    # POSTGRES_DB_TEST: str = os.getenv('POSTGRES_DB_TEST')

    # SQLALCHEMY_DATABASE_URL: str = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB}"


# engine = get_engine(settings.SQLALCHEMY_DATABASE_URL)
# AsyncSessionLocal = get_session_factory(engine)

# async def get_db():
#     async with AsyncSessionLocal() as session:
#         try:
#             yield session
#         finally:
#             await session.close()