import os
import uuid
import pytest

# Endpoints under test:
#   /api/v1/plugins/clear_cache/{plugin_id}
#   /api/v1/plugins/clear_semaphores/{plugin_id}
#   /api/v1/jobs/clear_semaphores/job/{job_id}


# ---------------------------------------------------------------------------
# 1) Clear plugin cache by plugin id
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clear_plugin_cache_by_plugin_id(backend_client, redis_service):
    plugin_id = 1001
    other_id  = 1002

    # Create cache-like keys for this plugin and another plugin
    keys_plugin = [
        f"plugin:{plugin_id}:foo",
        f"plugin:{plugin_id}:bar",
        f"plugin:{plugin_id}:baz",
    ]
    keys_other = [
        f"plugin:{other_id}:foo",
    ]

    for k in keys_plugin + keys_other:
        await redis_service.redis.set(k, "1")

    # Sanity: keys exist
    for k in keys_plugin + keys_other:
        assert await redis_service.redis.exists(k) == 1

    # Call backend
    resp = backend_client.delete(f"/api/v1/plugins/clear_cache/{plugin_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data.get("success") is True
    # total removed should be exactly our keys for this plugin
    assert data.get("removed") == len(keys_plugin)

    # Verify: plugin keys removed, other plugin keys kept
    for k in keys_plugin:
        assert await redis_service.redis.exists(k) == 0
    for k in keys_other:
        assert await redis_service.redis.exists(k) == 1


# ---------------------------------------------------------------------------
# 2) Clear semaphores by plugin id (removes live tokens + prunes job sets)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clear_plugin_semaphores_by_plugin_id(backend_client, redis_service):
    plugin_id = 4321
    sem_name  = f"plugin:{plugin_id}"
    job_id    = 54321

    # Acquire a few tokens with job-tracking enabled
    redis_service.job_id = job_id
    tokens = await redis_service.acquire_semaphores_batch(
        name=sem_name, n=3, max_locks=10
    )
    assert len(tokens) == 3

    sem_key   = f"semaphore:{sem_name}"
    owner_key = f"{sem_key}:owner"
    job_key   = f"job:{job_id}:semaphores"

    # Sanity: structures populated
    assert await redis_service.redis.zcard(owner_key) == 3
    assert await redis_service.redis.zcard(sem_key)   == 3
    members = await redis_service.redis.smembers(job_key)
    assert len(members) == 3

    # Call backend
    resp = backend_client.delete(f"/api/v1/plugins/clear_semaphores/{plugin_id}")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload.get("success") is True
    # Stats should reflect removal of our 3 tokens
    assert payload.get("owners_removed") == 3
    assert payload.get("semaphores_removed") == 3
    assert payload.get("job_members_removed") == 3

    # Verify: zsets empty, job tracking set pruned
    assert await redis_service.redis.zcard(owner_key) == 0
    assert await redis_service.redis.zcard(sem_key)   == 0
    assert await redis_service.redis.smembers(job_key) == set()


# ---------------------------------------------------------------------------
# 3) Clear semaphores by job id (removes any tokens tracked for that job)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clear_semaphores_by_job_id(backend_client, redis_service):
    plugin_id = 777
    sem_name  = f"plugin:{plugin_id}"
    job_id    = 12345

    # Acquire tokens under this job context
    redis_service.job_id = job_id
    tokens = await redis_service.acquire_semaphores_batch(
        name=sem_name, n=2, max_locks=10
    )
    assert len(tokens) == 2

    sem_key   = f"semaphore:{sem_name}"
    owner_key = f"{sem_key}:owner"
    job_key   = f"job:{job_id}:semaphores"

    # Sanity: structures populated
    assert await redis_service.redis.zcard(owner_key) == 2
    assert await redis_service.redis.zcard(sem_key)   == 2
    assert len(await redis_service.redis.smembers(job_key)) == 2

    # Call backend
    resp = backend_client.delete(f"/api/v1/jobs/clear_semaphores/job/{job_id}")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload.get("success") is True
    assert payload.get("job_id") == job_id
    assert payload.get("identifiers_removed") == 2

    # Verify: tokens gone, job set cleaned
    assert await redis_service.redis.zcard(owner_key) == 0
    assert await redis_service.redis.zcard(sem_key)   == 0
    assert await redis_service.redis.exists(job_key) in (0, False)
