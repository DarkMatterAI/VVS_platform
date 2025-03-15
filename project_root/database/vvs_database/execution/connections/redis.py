import os 
import uuid 
import json 
import time 
import asyncio 
import random 
from redis.asyncio import Redis
from typing import Optional, List, Dict, Any, Union 

from vvs_database.settings import settings 

        
class RedisService:
    """Redis service for caching, receiving message responses, and managing concurrency"""

    def __init__(self, 
                 redis_url: Optional[str]=None, 
                 cache_ttl: Optional[int]=None,
                 ):
        
        self.init(redis_url, cache_ttl)

    def init(self, 
             redis_url: Optional[str]=None, 
             cache_ttl: Optional[int]=None):
        
        if redis_url is None:
            redis_url = settings.REDIS_URL

        if cache_ttl is None:
            cache_ttl = int(os.getenv('REDIS_MESSAGE_TTL', 3600))

        self.redis_url = redis_url
        self.cache_ttl = cache_ttl
        self.redis = Redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self.semaphore_identifiers = {}  # Track acquired semaphores
        self.log_id = ''

    async def close(self):
        await self.redis.aclose()
    
    async def get_results(self, keys: List[str], delete: bool=False) -> Dict[str, Any]:
        """Get multiple results from Redis cache"""
        print(f"{self.log_id}: Checking redis with {len(keys)} keys")
        if not keys:
            return {}
        
        results = await self.redis.mget(keys)
        
        parsed_results = {}
        for key, result in zip(keys, results):
            if result:
                try:
                    parsed_results[key] = json.loads(result)
                    if delete:
                        await self.redis.delete(key)
                except json.JSONDecodeError:
                    print(f"{self.log_id}: Key {key} json decode error")
                    pass  # Skip invalid JSON

        n_found = len(parsed_results.keys())
        pct_found = n_found / len(keys)
        print(f"{self.log_id}: Found {n_found}/{len(keys)} keys, {pct_found:.3f} hit percent")
        
        return parsed_results
    
    async def set_results(self, results: Dict[str, Any]) -> None:
        """Set multiple results in Redis cache"""
        # if (not self.cache) or (not results):
        #     return
        if not results:
            return
        
        print(f"{self.log_id}: Setting {len(results.keys())} keys in cache")
            
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
        print(f"{self.log_id}: Acquiring semaphore for {name}")

        identifier = str(uuid.uuid4())
        semaphore_key = f"semaphore:{name}"
        counter_key = f"{semaphore_key}:counter"
        owner_key = f"{semaphore_key}:owner"
        
        current_backoff = initial_backoff
        
        for attempt in range(max_attempts):
            print(f"{self.log_id}: Acquiring semaphore for {name} - attempt {attempt}")
            now = time.time()

            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.zremrangebyscore(semaphore_key, "-inf", now - lock_timeout)
                pipe.zinterstore(owner_key, {owner_key: 1, semaphore_key: 0})
                pipe.incr(counter_key)
                counter = (await pipe.execute())[-1]
                print(f"{self.log_id}: Counter {counter}")

                pipe.zadd(semaphore_key, {identifier: now})
                pipe.zadd(owner_key, {identifier: counter})
                pipe.zrank(owner_key, identifier)
                rank = (await pipe.execute())[-1]
                print(f"{self.log_id}: Rank {rank}")

                if rank is not None and rank < max_locks:
                    # self.semaphore_identifiers[name] = identifier
                    print(f"{self.log_id}: Successfully acquired semaphore for {name}")
                    return True, identifier 
                else:
                    print(f"{self.log_id}: Unable to acquire semaphore for {name}")
                    print(f"{self.log_id}: Removing identifier")
                    pipe.zrem(semaphore_key, identifier)
                    pipe.zrem(owner_key, identifier)
                    await pipe.execute()

                    # Last attempt? Exit now
                    if attempt == max_attempts - 1:
                        print(f"{self.log_id}: Failed to acquire semaphore for {name}")
                        return False, ""
                    
                    # Add jitter (±20%) to prevent thundering herd
                    jitter = 0.8 + (random.random() * 0.4)  # 0.8-1.2
                    sleep_time = current_backoff * jitter
                    
                    # Sleep with backoff
                    print(f"{self.log_id}: Sleeping for {sleep_time}")
                    await asyncio.sleep(sleep_time)
                    
                    # Increase backoff for next attempt
                    current_backoff = min(current_backoff * backoff_factor, max_backoff)
        
        return False, ""  # This should never be reached but added for clarity

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

        print(f"{self.log_id}: Releasing {len(identifiers)} locks for {name}")
            
        semaphore_key = f"semaphore:{name}"
        owner_key = f"{semaphore_key}:owner"
        
        # Remove our lock from both sets
        async with self.redis.pipeline(transaction=True) as pipe:
            for identifier in identifiers:
                pipe.zrem(semaphore_key, identifier)
                pipe.zrem(owner_key, identifier)
            results = await pipe.execute()

        released = sum(results[::2])  # Every other result is from zrem on semaphore_key
        print(f"{self.log_id}: Released {released} locks for {name}")
        return released 
