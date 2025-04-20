import os 
import uuid 
import json 
import time 
import asyncio 
import random 
from redis.asyncio import Redis
from typing import Optional, List, Dict, Any, Union 

from vvs_database.schemas import RedisConnection
from vvs_database import logging


class RedisService:
    """Redis service for caching, receiving message responses, and managing concurrency"""

    def __init__(self, 
                 redis_connection: RedisConnection,
                 verbose: bool=False):
        self.init(redis_connection)
        self.verbose = verbose 
    
    def init(self, redis_connection: RedisConnection):
        self.redis_url = redis_connection.redis_url
        self.cache_ttl = redis_connection.cache_ttl
        self.redis = None 
        self.semaphore_identifiers = {}
        self.log_id = ''

    def init_redis_connection(self):
        if self.redis is None:
            self.redis = Redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)

    async def close(self):
        if self.redis is not None:
            await self.redis.aclose()
    
    async def get_results(self, keys: List[str], delete: bool=False) -> Dict[str, Any]:
        """Get multiple results from Redis cache"""
        logging.info(f"{self.log_id}: Checking redis with {len(keys)} keys")
        if not keys:
            return {}
        
        self.init_redis_connection()
        results = await self.redis.mget(keys)
        
        parsed_results = {}
        for key, result in zip(keys, results):
            if result:
                try:
                    parsed_results[key] = json.loads(result)
                    if delete:
                        await self.redis.delete(key)
                except json.JSONDecodeError:
                    logging.error(f"{self.log_id}: Key {key} json decode error")
                    pass  # Skip invalid JSON

        n_found = len(parsed_results.keys())
        pct_found = n_found / len(keys)
        logging.info(f"{self.log_id}: Found {n_found}/{len(keys)} keys, {pct_found:.3f} hit percent")
        
        return parsed_results
    
    async def set_results(self, results: Dict[str, Any]) -> None:
        """Set multiple results in Redis cache"""
        if not results:
            return
        
        logging.info(f"{self.log_id}: Setting {len(results.keys())} keys in cache")
            
        self.init_redis_connection()
        pipeline = self.redis.pipeline()
        
        for key, result in results.items():
            result_json = json.dumps(result.model_dump())
            pipeline.set(key, result_json, ex=self.cache_ttl)
        
        await pipeline.execute()
        return
    
    async def acquire_semaphore(
        self, 
        name: str, 
        max_locks: int, 
        lock_timeout: int = 30,
        max_attempts: int = 10,
        initial_backoff: float = 0.1,
        max_backoff: float = 5.0,
        backoff_factor: float = 2.0
    ) -> bool:
        """
        Try to acquire a slot in the semaphore with retry logic.
        
        Args:
            name: Name of the semaphore
            max_locks: Maximum number of concurrent locks
            lock_timeout: How long until the lock expires in seconds
            max_attempts: Maximum number of acquisition attempts
            initial_backoff: Initial backoff time in seconds
            max_backoff: Maximum backoff time in seconds
            backoff_factor: Multiplicative factor for backoff
            
        Returns:
            bool: True if semaphore was acquired, False otherwise
        """
        if self.verbose:
            logging.info(f"{self.log_id}: Acquiring semaphore for {name}")

        identifier = str(uuid.uuid4())
        semaphore_key = f"semaphore:{name}"
        counter_key = f"{semaphore_key}:counter"
        owner_key = f"{semaphore_key}:owner"
        current_backoff = initial_backoff
        self.init_redis_connection()
        
        for attempt in range(max_attempts):
            if self.verbose:
                logging.info(f"{self.log_id}: Acquiring semaphore for {name} - attempt {attempt}")
            now = time.time()

            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.zremrangebyscore(semaphore_key, "-inf", now - lock_timeout)
                pipe.zinterstore(owner_key, {owner_key: 1, semaphore_key: 0})
                pipe.incr(counter_key)
                counter = (await pipe.execute())[-1]

                pipe.zadd(semaphore_key, {identifier: now})
                pipe.zadd(owner_key, {identifier: counter})
                pipe.zrank(owner_key, identifier)
                rank = (await pipe.execute())[-1]

                if rank is not None and rank < max_locks:
                    if self.verbose:
                        logging.info(f"{self.log_id}: Successfully acquired semaphore for {name}")
                    return True, identifier 
                else:
                    logging.info(f"{self.log_id}: Unable to acquire semaphore for {name}")
                    logging.info(f"{self.log_id}: Removing identifier")
                    pipe.zrem(semaphore_key, identifier)
                    pipe.zrem(owner_key, identifier)
                    await pipe.execute()

                    # Last attempt? Exit now
                    if attempt == max_attempts - 1:
                        logging.error(f"{self.log_id}: Failed to acquire semaphore for {name}")
                        return False, ""
                    
                    # Add jitter (±20%) to prevent thundering herd
                    jitter = 0.8 + (random.random() * 0.4)  # 0.8-1.2
                    sleep_time = current_backoff * jitter
                    
                    # Sleep with backoff
                    if self.verbose:
                        logging.info(f"{self.log_id}: Sleeping for {sleep_time}")
                    await asyncio.sleep(sleep_time)
                    
                    # Increase backoff for next attempt
                    current_backoff = min(current_backoff * backoff_factor, max_backoff)
        
        return False, ""  # This should never be reached but added for clarity

    async def acquire_semaphores_batch(
        self,
        name: str,
        n: int,                 # how many tokens we’d like
        max_locks: int,
        lock_timeout: int = 30,
    ) -> List[str]:             # identifiers we actually got
        """
        Atomically try to grab up to *n* semaphore slots.
        Returns the list of identifiers actually acquired.
        """
        logging.info(f"{self.log_id}: New batch confirmed")
        if n <= 0:
            return []

        self.init_redis_connection()
        now = time.time()
        semaphore_key = f"semaphore:{name}"
        owner_key     = f"{semaphore_key}:owner"

        # ----- phase 1: clean up & discover capacity -----
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(semaphore_key, "-inf", now - lock_timeout)
            pipe.zinterstore(owner_key, {owner_key: 1, semaphore_key: 0})
            pipe.zcard(owner_key)                       # current holders
            current = (await pipe.execute())[-1]

        free = max(0, max_locks - current)
        if free == 0:
            return []                                   # nothing available

        grab = min(free, n)
        identifiers = [str(uuid.uuid4()) for _ in range(grab)]

        # ----- phase 2: claim the slots we just discovered -----
        async with self.redis.pipeline(transaction=True) as pipe:
            for i, ident in enumerate(identifiers, start=1):
                pipe.zadd(semaphore_key, {ident: now})
                pipe.zadd(owner_key,    {ident: current + i})
            await pipe.execute()

        return identifiers

    async def release_semaphore(self, name: str, identifiers: Union[str, List[str]]) -> bool:
        """
        Release a previously acquired semaphore.
        
        Args:
            name: Name of the semaphore
            
        Returns:
            bool: True if semaphore was released, False if it didn't exist or was expired
        """

        if type(identifiers) == str:
            identifiers = [identifiers]

        logging.info(f"{self.log_id}: Releasing {len(identifiers)} locks for {name}")
            
        semaphore_key = f"semaphore:{name}"
        owner_key = f"{semaphore_key}:owner"
        self.init_redis_connection()
        
        # Remove our lock from both sets
        async with self.redis.pipeline(transaction=True) as pipe:
            for identifier in identifiers:
                pipe.zrem(semaphore_key, identifier)
                pipe.zrem(owner_key, identifier)
            results = await pipe.execute()

        released = sum(results[::2])  # Every other result is from zrem on semaphore_key
        if self.verbose:
            logging.info(f"{self.log_id}: Released {released} locks for {name}")
        return released 
