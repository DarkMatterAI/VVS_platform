from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, ConfigDict
from typing import Optional 

from vvs_database.execution.connections.database import DatabaseService
from vvs_database.execution.connections.redis import RedisService, RedisConnection
from vvs_database.execution.connections.rabbitmq import RabbitMQService, RabbitMQConnection

class Connections(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    db_service: DatabaseService
    redis_service: RedisService
    rabbitmq_service: RabbitMQService

    def init_log_id(self, log_id: str):
        self.db_service.log_id = f"{log_id}:DB"
        self.redis_service.log_id = f"{log_id}:Redis"
        self.rabbitmq_service.log_id = f"{log_id}:Rabbitmq"

    async def close(self):
        await self.redis_service.close()
        await self.rabbitmq_service.close()

def get_connections(db_session: AsyncSession,
                    redis_connection: Optional[RedisConnection]=None,
                    rabbitmq_connection: Optional[RabbitMQConnection]=None):
    db_service = DatabaseService(db_session)

    if redis_connection is None:
        redis_connection = RedisConnection()

    redis_service = RedisService(redis_connection)

    if rabbitmq_connection is None:
        rabbitmq_connection = RabbitMQConnection()

    rabbitmq_service = RabbitMQService(rabbitmq_connection)

    connections = Connections(db_service=db_service,
                              redis_service=redis_service,
                              rabbitmq_service=rabbitmq_service)
    return connections 
