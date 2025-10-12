from typing import Optional, List, Dict, Any, Tuple
from aioredis import Redis

async def delete_redis_keys_batch(keys, redis_client):
    if keys:
        if hasattr(redis_client, 'unlink'):
            deleted = await redis_client.unlink(*keys)
        else:
            deleted = await redis_client.delete(*keys)
    
async def clear_plugin_cache(plugin_id: int, redis_client: Redis, batch_size: int=500):
    pattern = f"plugin:{plugin_id}:*"

    total_deleted = 0
    keys_batch = []

    async for key in redis_client.scan_iter(match=pattern):
        keys_batch.append(key)

        if len(keys_batch) >= batch_size:
            await delete_redis_keys_batch(keys_batch, redis_client)
            total_deleted += len(keys_batch)
            keys_batch = []
            
    await delete_redis_keys_batch(keys_batch, redis_client)
    total_deleted += len(keys_batch)

    return total_deleted

async def _prune_job_set_for_plugin(
    rds: Redis, job_key: str, plugin_name: str, *, scan_count: int = 1000
) -> int:
    """Remove all members with prefix 'plugin_name:' from one job set."""
    prefix = f"{plugin_name}:"
    removed_total = 0
    cursor = 0
    while True:
        cursor, members = await rds.sscan(job_key, cursor=cursor, match=f"{prefix}*", count=scan_count)
        if members:
            removed_total += await rds.srem(job_key, *members)
        if cursor == 0:
            break
    return removed_total

async def clear_plugin_semaphores(
    plugin_id: int,
    redis_client: Redis,
    *,
    job_ids: Optional[List[int]] = None,   # if None, SCAN all job sets
    scan_count: int = 1000,
) -> Dict[str, int]:
    """
    Clear all semaphore tokens for plugin:{plugin_id} and prune any matching
    entries from job:*:semaphores sets.

    Returns stats:
      {
        "owners_removed": <int>,
        "semaphores_removed": <int>,
        "job_members_removed": <int>,
        "job_sets_touched": <int>,
      }
    """
    plugin_name = f"plugin:{plugin_id}"
    sem_key   = f"semaphore:{plugin_name}"
    owner_key = f"{sem_key}:owner"

    owners_removed = 0
    sem_removed    = 0
    job_removed    = 0
    job_sets_touched = 0

    # 1) Remove all live tokens for this plugin
    idents = await redis_client.zrange(owner_key, 0, -1)
    if not idents:
        # fallback: if owner set empty, check base zset for stragglers
        base = await redis_client.zrange(sem_key, 0, -1)
        idents = base

    if idents:
        async with redis_client.pipeline(transaction=True) as pipe:
            for ident in idents:
                pipe.zrem(owner_key, ident)
                pipe.zrem(sem_key, ident)
            res = await pipe.execute()
        owners_removed = sum(res[0::2])  # owner zrem results at even indices
        sem_removed    = sum(res[1::2])  # base zset  zrem results at odd indices

    # 2) Prune job tracking sets (job:{id}:semaphores) for members like "plugin:{id}:{uuid}"
    if job_ids is not None:
        for jid in job_ids:
            job_key = f"job:{jid}:semaphores"
            if await redis_client.exists(job_key):
                removed = await _prune_job_set_for_plugin(redis_client, job_key, plugin_name, scan_count=scan_count)
                if removed:
                    job_sets_touched += 1
                    job_removed += removed
    else:
        cursor = 0
        while True:
            cursor, keys = await redis_client.scan(cursor=cursor, match="job:*:semaphores", count=scan_count)
            for job_key in keys:
                removed = await _prune_job_set_for_plugin(redis_client, job_key, plugin_name, scan_count=scan_count)
                if removed:
                    job_sets_touched += 1
                    job_removed += removed
            if cursor == 0:
                break

    return {
        "owners_removed": owners_removed,
        "semaphores_removed": sem_removed,
        "job_members_removed": job_removed,
        "job_sets_touched": job_sets_touched,
    }


async def clear_job_semaphores(
    job_id: int,
    redis_client: Redis,
) -> int:
    """
    Remove all tokens recorded in job:{job_id}:semaphores and their live entries
    in the corresponding semaphore zsets. Returns number of identifiers removed.
    """
    job_key = f"job:{job_id}:semaphores"
    members = await redis_client.smembers(job_key)
    if not members:
        return 0

    removed = 0
    async with redis_client.pipeline(transaction=True) as pipe:
        for m in members:
            # m is "plugin:{id}:{uuid}" ; split from RIGHT
            try:
                plugin_name, ident = m.rsplit(":", 1)
            except ValueError:
                continue
            sem_key   = f"semaphore:{plugin_name}"
            owner_key = f"{sem_key}:owner"
            pipe.zrem(sem_key, ident)
            pipe.zrem(owner_key, ident)
            removed += 1
        pipe.delete(job_key)
        await pipe.execute()

    return removed