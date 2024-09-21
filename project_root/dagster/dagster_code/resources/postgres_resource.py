
from dagster import ConfigurableResource
from sqlalchemy import create_engine

class PostgresResourceConfig(ConfigurableResource):
    host: str 
    db_name: str 
    username: str 
    password: str 

    def get_engine(self):
        db_url = f"postgresql://{self.username}:{self.password}@{self.host}/{self.db_name}"
        engine = create_engine(db_url)
        return engine 

