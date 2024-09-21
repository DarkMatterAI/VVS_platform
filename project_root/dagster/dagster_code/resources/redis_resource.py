import redis 
from dagster import ConfigurableResource

class RedisResourceConfig(ConfigurableResource):
    host: str 
    port: int
    password: str 
    db: int 
    
    def get_client(self):
        redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.db
        )
        return redis_client
