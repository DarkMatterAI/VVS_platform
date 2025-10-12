import dagster as dg
from dagster_aws.s3 import S3Resource, S3PickleIOManager

import time 
import asyncio
import uuid 

from qdrant_client import AsyncQdrantClient, models

from vvs_database.core import get_engine, get_session_factory
from vvs_database.schemas import RabbitMQConnection, RedisConnection
from vvs_database.execution.connections import DatabaseService, RabbitMQService, RedisService

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

    def get_service(self):
        session = self.get_db()
        db_service = DatabaseService(session)
        return db_service
    
class RabbitMQResource(dg.ConfigurableResource):
    host: str 
    port: str 
    username: str 
    password: str 
    exchange: str 

    def to_model(self):
        return RabbitMQConnection(host=self.host,
                                  port=self.port,
                                  username=self.username,
                                  password=self.password,
                                  exchange=self.exchange)
    
    def get_service(self):
        connection = self.to_model()
        service = RabbitMQService(connection)
        return service 

class RedisResource(dg.ConfigurableResource):
    host: str 
    port: str
    password: str 
    db: str 
    cache_ttl: str 

    def to_model(self):
        url =  f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return RedisConnection(redis_url=url, cache_ttl=int(self.cache_ttl))
    
    def get_service(self):
        connection = self.to_model()
        return RedisService(connection)

class QdrantResource(dg.ConfigurableResource):
    port: str 
    grpc_port: str 
    upload_job_chunksize: int 
    upload_batch_size: int 
    upload_processes: int 
    upload_max_retries: int 
    upload_ping: int 
    indexing_timeout: int 

    def get_service(self):
        client = AsyncQdrantClient(location='qdrant',
                                   port=int(self.port),
                                   grpc_port=int(self.grpc_port),
                                   prefer_grpc=True,
                                   timeout=60)
        return client 
    
    async def set_indexing_threshold(self,
                                     logging,
                                     qdrant_client: AsyncQdrantClient,
                                     collection_name: str,
                                     threshold: int):
        logging.info(f'Qdrant collection {collection_name}: Setting qdrant indexing threshold to {threshold}')
        await qdrant_client.update_collection(collection_name,
                                            optimizers_config=models.OptimizersConfigDiff(
                                                indexing_threshold=threshold))
        
    async def get_m_params(self,
                           logging,
                           qdrant_client: AsyncQdrantClient,
                           collection_name: str):
        collection_info = await self.get_collection_info(logging, qdrant_client, collection_name)
        config = collection_info["config"]

        m_dict = {
            "hnsw_m": config.get("hnsw_config", {}).get("m", None),
            "vector_m": {}
        }

        vector_config = config.get("params", {}).get("vectors", {})
        for k,v in vector_config.items():
            m_dict["vector_m"][k] = v.get("hnsw_config", {}).get("m", None)

        update_dict = {"collection_name": collection_name}
        return_dict = {"collection_name": collection_name}

        if m_dict["hnsw_m"] is not None:
            update_dict["hnsw_config"] = models.HnswConfigDiff(m=0)
            return_dict["hnsw_config"] = models.HnswConfigDiff(m=m_dict["hnsw_m"])
            

        update_dict["vectors_config"] = {}
        return_dict["vectors_config"] = {}
        for k,m in m_dict["vector_m"].items():
            if m is None:
                continue 
                
            
            update_dict["vectors_config"][k] = models.VectorParamsDiff(hnsw_config=models.HnswConfigDiff(m=0))
            return_dict["vectors_config"][k] = models.VectorParamsDiff(hnsw_config=models.HnswConfigDiff(m=m))
        
        return update_dict, return_dict 

        
    # async def set_hnsw_m(self,
    #                      logging,
    #                      qdrant_client: AsyncQdrantClient,
    #                      collection_name: str,
    #                      m: int):
    #     logging.info(f'Qdrant collection {collection_name}: Setting HNSW m param to {m}')

    #     await qdrant_client.update_collection(collection_name=collection_name,
    #                                           hnsw_config=models.HnswConfigDiff(m=m))

    async def set_hnsw_m(self,
                         logging,
                         qdrant_client: AsyncQdrantClient,
                         update_dict: dict):
        logging.info(f"Qdrant collection {update_dict['collection_name']}: Setting HNSW m params: {update_dict}")
        await qdrant_client.update_collection(**update_dict)
    
    async def index_sleep(self, 
                          logging, 
                          qdrant_client: AsyncQdrantClient,
                          collection_name: str,
                          update_dict: dict,
                          ) -> dict:
        logging.info(f'Qdrant collection {collection_name}: building index')
        index_start = time.time()
        # set threshold to 1 to start indexing
        # await self.set_indexing_threshold(logging, qdrant_client, collection_name, 20000)
        await self.set_hnsw_m(logging, qdrant_client, update_dict)

        # wait for qdrant internals to change collection status
        await asyncio.sleep(2.0)

        index_log = {'index_timeout' : False,
                     'index_error' : False}
        
        while True:
            elapsed = time.time() - index_start 
            index_log['index_time'] = elapsed
            if elapsed > self.indexing_timeout:
                index_log['index_timeout'] = True 
                return index_log 
            
            collection_data = await qdrant_client.get_collection(collection_name)
            status = collection_data.status
            if status == 'green':
                logging.info(f'Qdrant collection {collection_name}: building index complete')
                return index_log 

            elif status == 'yellow':
                logging.info(f'Qdrant collection {collection_name}: waiting on index, {elapsed} elapsed')
                await asyncio.sleep(self.upload_ping)

            else:
                logging.error(f'Qdrant collection {collection_name}: index build error, ' \
                              f'status {status}, collection data {collection_data}')
                index_log['index_error'] = True 
                return index_log 
            
    def qdrant_records_to_points(self, records: list[dict]):
        points = []
        failed = []

        for record in records:
            payload = record["item_data"]
            if not record["valid"]:
                failed.append(payload)
                continue 

            point = models.PointStruct(id=str(uuid.uuid4()),
                                    payload=payload,
                                    vector={f"embedding_{plugin_id}" : embedding 
                                            for plugin_id, embedding in record['embeddings'].items()})
            points.append(point)
        return points, failed 
    
    def upload_points(self,
                      logging,
                      qdrant_client: AsyncQdrantClient,
                      collection_name: str,
                      points: list[models.PointStruct]):
        logging.info(f"Qdrant collection {collection_name}: starting upload of {len(points)} points")
        response = qdrant_client.upload_points(collection_name=collection_name,
                                               points=points,
                                               parallel=self.upload_processes,
                                               max_retries=self.upload_max_retries,
                                               batch_size=self.upload_batch_size)
        return response 

    async def get_collection_info(self,
                            logging,
                            qdrant_client: AsyncQdrantClient,
                            collection_name: str):
        logging.info(f"Qdrant collection {collection_name}: fetching collection info")
        response = await qdrant_client.get_collection(collection_name)
        response = response.model_dump()
        return response 


S3 = S3Resource(region_name=dg.EnvVar('S3_REGION'),
                endpoint_url=dg.EnvVar('S3_URL'),
                # TODO: get s3 env vars working 
                # profile_name=dg.EnvVar('S3_PROFILE_NAME'),
                # use_ssl=dg.EnvVar('S3_USE_SSL'),
                # verify=dg.EnvVar('S3_VERIFY_SSL'),
                aws_access_key_id=dg.EnvVar('S3_ACCESS_KEY'),
                aws_secret_access_key=dg.EnvVar('S3_SECRET_KEY'),
                aws_session_token=dg.EnvVar('S3_SESSION_TOKEN'))

RESOURCE_DEFAULTS = {
    "postgres_resource": PostgresResource(postgres_user=dg.EnvVar("POSTGRES_USER"),
                                          postgres_password=dg.EnvVar("POSTGRES_PASSWORD"),
                                          postgres_db=dg.EnvVar("POSTGRES_DB")),
    "rabbitmq_resource": RabbitMQResource(host=dg.EnvVar('RABBITMQ_HOST'),
                                          port=dg.EnvVar('RABBITMQ_PORT'),
                                          username=dg.EnvVar('RABBITMQ_DEFAULT_USER'),
                                          password=dg.EnvVar('RABBITMQ_DEFAULT_PASS'),
                                          exchange=dg.EnvVar('RABBITMQ_EXCHANGE_NAME')),
    "redis_resource": RedisResource(host=dg.EnvVar('REDIS_HOST'),
                                    port=dg.EnvVar('REDIS_PORT'),
                                    password=dg.EnvVar('REDIS_PASSWORD'),
                                    db=dg.EnvVar('REDIS_DB'),
                                    cache_ttl=dg.EnvVar('REDIS_MESSAGE_TTL')),
    "qdrant_resource": QdrantResource(port=dg.EnvVar('QDRANT__SERVICE__HTTP_PORT'),
                                      grpc_port=dg.EnvVar('QDRANT__SERVICE__GRPC_PORT'),
                                      upload_job_chunksize=dg.EnvVar.int('QDRANT_UPLOAD_JOB_CHUNKSIZE'),
                                      upload_batch_size=dg.EnvVar.int('QDRANT_UPLOAD_BATCH_SIZE'),
                                      upload_processes=dg.EnvVar.int('QDRANT_UPLOAD_PROCESSES'),
                                      upload_max_retries=dg.EnvVar.int('QDRANT_UPLOAD_MAX_RETRIES'),
                                      upload_ping=dg.EnvVar.int('QDRANT_UPLOAD_PING'),
                                      indexing_timeout=dg.EnvVar.int('QDRANT_INDEXING_TIMEOUT')),
    "s3_resource": S3,
    "io_manager": S3PickleIOManager(s3_resource=S3,
                                    s3_bucket=dg.EnvVar('S3_BUCKET')),
}

