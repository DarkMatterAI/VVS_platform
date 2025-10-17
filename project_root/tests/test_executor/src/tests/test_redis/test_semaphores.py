import pytest 
import uuid 

# ---------------------------------------------------------------------------
# 1.  Basic acquire / release without job‑tracking
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_semaphore_acquire_release_basic(redis_service):
    sem_name   = f"test:{uuid.uuid4()}"
    ok, ident  = await redis_service.acquire_semaphore(
        name=sem_name, max_locks=1, max_attempts=1
    )
    assert ok and ident

    # identifier should be present in the semaphore zset
    cnt = await redis_service.redis.zcard(f"semaphore:{sem_name}")
    assert cnt == 1

    rel = await redis_service.release_semaphore(sem_name, ident)
    assert rel == 1
    cnt_after = await redis_service.redis.zcard(f"semaphore:{sem_name}")
    assert cnt_after == 0


# ---------------------------------------------------------------------------
# 2.  Job‑linked identifiers are tracked & cleared
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_semaphore_job_tracking_and_cleanup(redis_service):
    # set a job id so acquisitions are recorded
    job_id               = 54321
    redis_service.job_id = job_id

    sem_name       = f"jobtest:{uuid.uuid4()}"
    tokens         = await redis_service.acquire_semaphores_batch(
        name=sem_name, n=3, max_locks=3
    )
    assert len(tokens) == 3

    # job set should contain 3 entries
    job_key = f"job:{job_id}:semaphores"
    members = await redis_service.redis.smembers(job_key)
    assert len(members) == 3
    for t in tokens:
        assert f"{sem_name}:{t}" in members

    # release ONE token -> job set shrinks by 1
    await redis_service.release_semaphore(sem_name, tokens[0])
    members_after = await redis_service.redis.smembers(job_key)
    assert len(members_after) == 2
    assert f"{sem_name}:{tokens[0]}" not in members_after

    # invoke cleanup -> remaining tokens removed + set deleted
    removed = await redis_service.clear_job_semaphores(job_id)
    assert removed == 2
    assert not await redis_service.redis.exists(job_key)
    cnt = await redis_service.redis.zcard(f"semaphore:{sem_name}")
    assert cnt == 0

# ---------------------------------------------------------------------------
# 3.  clear_plugin_semaphores: removes live tokens + prunes job sets
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clear_plugin_semaphores_with_job_sets(redis_service):
    plugin_id = 12345
    sem_name  = f"plugin:{plugin_id}"

    # Acquire a few tokens *with* job tracking enabled
    job_id = 12345
    redis_service.job_id = job_id
    tokens = await redis_service.acquire_semaphores_batch(
        name=sem_name, n=3, max_locks=10
    )
    assert len(tokens) == 3

    # Verify live structures and job set are populated
    sem_key   = f"semaphore:{sem_name}"
    owner_key = f"{sem_key}:owner"
    job_key   = f"job:{job_id}:semaphores"

    assert await redis_service.redis.zcard(owner_key) == 3
    assert await redis_service.redis.zcard(sem_key)   == 3
    members = await redis_service.redis.smembers(job_key)
    assert len(members) == 3
    for t in tokens:
        assert f"{sem_name}:{t}" in members

    # Now simulate running clear *outside* any job context
    redis_service.job_id = None
    stats = await redis_service.clear_plugin_semaphores(plugin_id)

    # Live semaphore sets should be empty
    assert await redis_service.redis.zcard(owner_key) == 0
    assert await redis_service.redis.zcard(sem_key)   == 0

    # Job set should have had those members pruned (key may remain as empty set)
    members_after = await redis_service.redis.smembers(job_key)
    assert members_after == set()

    # Sanity-check counters
    assert stats["owners_removed"] == 3
    assert stats["semaphores_removed"] == 3
    assert stats["job_members_removed"] == 3
    assert stats["job_sets_touched"] >= 1  # exactly 1 in this test


# ---------------------------------------------------------------------------
# 4.  clear_plugin_semaphores: no job sets present (job_id never set)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clear_plugin_semaphores_without_job_sets(redis_service):
    plugin_id = 24680
    sem_name  = f"plugin:{plugin_id}"

    # Acquire tokens *without* setting redis_service.job_id
    tokens = await redis_service.acquire_semaphores_batch(
        name=sem_name, n=2, max_locks=10
    )
    assert len(tokens) == 2

    sem_key   = f"semaphore:{sem_name}"
    owner_key = f"{sem_key}:owner"
    assert await redis_service.redis.zcard(owner_key) == 2
    assert await redis_service.redis.zcard(sem_key)   == 2

    # There should be no job:* tracking sets in this scenario
    stats = await redis_service.clear_plugin_semaphores(plugin_id)

    # Live sets cleared
    assert await redis_service.redis.zcard(owner_key) == 0
    assert await redis_service.redis.zcard(sem_key)   == 0

    # Counters should reflect no job-member removals
    assert stats["owners_removed"] == 2
    assert stats["semaphores_removed"] == 2
    assert stats["job_members_removed"] == 0
    # job_sets_touched may be 0 (no job sets existed)
    assert stats["job_sets_touched"] in (0, 1)  # allow 0; be lenient across redis versions
    