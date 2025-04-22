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
    job_id         = str(uuid.uuid4())
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

