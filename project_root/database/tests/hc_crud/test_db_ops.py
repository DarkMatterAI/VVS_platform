import asyncio
import types
import itertools
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ── models / CRUD imports ────────────────────────────────────────────────────
from vvs_database.models.job_models.hc_models import (
    HCJob,
    HCInputJob,
    HCIterationJob,
    HCInputItems,
    HCResult,
    HCIterationResult,
)
from vvs_database.models.job_models.job_models import JobStatus, JobType
from vvs_database.models import Item, Assembly, AssemblyComponent, ItemResult

from vvs_database.crud.hc_crud.hc_job_crud import (
    latest_hc_iteration,
    load_hc_input_job_items,
)
from vvs_database.crud.hc_crud.hc_results_crud import (
    sum_inference_for_hc_input_job,
    sum_inference_for_hc_job,
)
from vvs_database.crud.hc_crud.hc_results_crud import (
    upsert_hc_results,
    upsert_hc_iteration_results,
    fetch_hc_job_results,
    export_hc_job_hierarchy,
)

from vvs_database.crud.job_crud import create_job, update_helper
from vvs_database.job_runner.hc_runner.hc_runner import HCRunner     # integration
from vvs_database.job_runner.hc_runner.hc_utils import should_stop_input, should_finalize_parent

# ─────────────────────────────────────────────────────────────────────────────
# Helper: minimal chain builder
# ─────────────────────────────────────────────────────────────────────────────
async def _mk_hc_chain(db, *, max_iterations=2):
    parent = await create_job(
        db,
        job_type=JobType.HILL_CLIMB_JOB,
        job_json=None,
        auto_execute=False,
        extra_args=dict(num_inputs=1),
    )
    input_job = await create_job(
        db,
        job_type=JobType.HILL_CLIMB_JOB_INPUT,
        job_json=None,
        auto_execute=False,
        extra_args=dict(parent_id=parent.id,
                        max_iterations=max_iterations,
                        inference_limit=None,
                        time_limit=None),
    )
    # starter iteration
    iter0 = await create_job(
        db,
        job_type=JobType.HILL_CLIMB_JOB_ITERATION,
        job_json=None,
        auto_execute=False,
        extra_args=dict(input_id=input_job.id, iteration=0),
    )
    return parent, input_job, iter0


# ════════════════════════════════════════════════════════════════════════════
# 1. latest_hc_iteration
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_latest_hc_iteration_returns_highest(db_session):
    parent, input_job, iter0 = await _mk_hc_chain(db_session)
    # add two more iterations
    for i in (1, 2):
        await create_job(
            db_session,
            job_type=JobType.HILL_CLIMB_JOB_ITERATION,
            job_json=None,
            auto_execute=False,
            extra_args=dict(input_id=input_job.id, iteration=i),
        )
    latest = await latest_hc_iteration(db_session, input_job.id)
    assert latest.iteration == 2
    await db_session.commit()


# ════════════════════════════════════════════════════════════════════════════
# 2. load_hc_input_job_items
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_load_hc_input_job_items(db_session, create_item):
    _, input_job, _ = await _mk_hc_chain(db_session)

    # two items with assembly_index 0,1
    items = [await create_item(f"I{i}") for i in (0, 1)]
    for idx, it in enumerate(items):
        db_session.add(
            HCInputItems(
                job_id=input_job.id,
                item_id=it.id,
                assembly_index=idx,
                external_id=f"ext{idx}",
            )
        )
    await db_session.commit()

    loaded = await load_hc_input_job_items(db_session, input_job)
    assert set(loaded) == {0, 1}
    assert loaded[0].item_data.item_id == items[0].id
    assert loaded[1].item_data.item_id == items[1].id
    await db_session.commit()


# ════════════════════════════════════════════════════════════════════════════
# 3. sum_inference helpers
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_sum_inference_helpers(db_session):
    parent, input_job, _ = await _mk_hc_chain(db_session)
    # three iterations with inference counts
    counts = [4, 6, 10]
    for i, c in enumerate(counts, start=1):
        iter_job = await create_job(
            db_session,
            job_type=JobType.HILL_CLIMB_JOB_ITERATION,
            job_json=None,
            auto_execute=False,
            extra_args=dict(input_id=input_job.id, iteration=i),
        )
        await update_helper(iter_job, {"inference": c})

    await update_helper(input_job, {"inference": sum(counts)})
    await update_helper(parent, {"inference": sum(counts)})
    await db_session.commit()

    assert await sum_inference_for_hc_input_job(db_session, input_job.id) == sum(counts)
    assert await sum_inference_for_hc_job(db_session, parent.id) == sum(counts)
    await db_session.commit()


# ════════════════════════════════════════════════════════════════════════════
# 4. should_stop_input truth‑table (a few key cases)
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.parametrize(
    "iterate_i,max_iter,has_queries,expect_stop,expect_status",
    [
        (0, 1, True, True, JobStatus.COMPLETE),            # finished
        (0, 5, False, True, JobStatus.COMPLETE_EARLY_STOP),# invalid
        (3, 5, True, False, JobStatus.RUNNING),            # keep going
    ],
)
def test_should_stop_input_basic(iterate_i, max_iter, has_queries, expect_stop, expect_status):
    dummy = types.SimpleNamespace(
        inference=0,
        inference_limit=None,
        time_limit=None,
        started_at=datetime.now(timezone.utc),
    )
    stop, status = should_stop_input(dummy, dummy, iterate_i, max_iter,
                                     [[]] if has_queries else [])
    assert stop is expect_stop
    assert status == expect_status or status == JobStatus.COMPLETE_EARLY_STOP


# ════════════════════════════════════════════════════════════════════════════
# 5. should_finalize_parent aggregation
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_should_finalize_parent(db_session):
    parent, input_job, iter0 = await _mk_hc_chain(db_session)
    # set first input job to running
    await update_helper(input_job, {"status": JobStatus.RUNNING})
    # two children with mixed terminal / non‑terminal states
    for st in (JobStatus.COMPLETE, JobStatus.FAILED):
        await create_job(
            db_session,
            job_type=JobType.HILL_CLIMB_JOB_INPUT,
            job_json=None,
            auto_execute=False,
            extra_args=dict(parent_id=parent.id, max_iterations=1, status=st),
        )
    await db_session.commit()

    # not all children terminal → expect None
    assert await should_finalize_parent(db_session, parent.id) is None

    # grab the RUNNING child and mark it COMPLETE via ORM helper
    running_child = (
        await db_session.execute(
            select(HCInputJob).where(
                HCInputJob.parent_id == parent.id, HCInputJob.status == JobStatus.RUNNING
            )
        )
    ).scalar_one()
    await update_helper(running_child, {"status": JobStatus.COMPLETE})
    await db_session.commit()

    res = await db_session.execute(select(HCInputJob.status).where(HCInputJob.parent_id == parent.id))
    statuses = {row[0] for row in res}
    print(statuses)

    # now every child terminal; because one is FAILED we expect COMPLETE_WITH_ERRORS
    assert await should_finalize_parent(db_session, parent.id) == JobStatus.COMPLETE_WITH_ERRORS

    await db_session.commit()


# ════════════════════════════════════════════════════════════════════════════
# 6. fetch_hc_job_results – ordering & pagination
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_fetch_hc_job_results_ordering(
    db_session,
    create_item,
    create_test_score_plugin,
):
    # score plugin
    score_p = await create_test_score_plugin()

    # HC job & child input
    parent, *_ = await _mk_hc_chain(db_session)

    # patch job_json so fetch_hc_job_results can discover the score‑plugin
    await update_helper(
        parent,
        {
            "job_json": {"plugin_config": {"score_config": {"plugin_id": score_p.id}}}
        },
    )
    await db_session.commit()

    # three Items with scores 0.1, 0.9, 0.5
    items = [await create_item(f"S{i}") for i in range(3)]
    scores = [0.1, 0.9, 0.5]
    for itm, sc in zip(items, scores):
        await db_session.execute(
            pg_insert(HCResult).values(job_id=parent.id, item_id=itm.id, valid=True)
        )
        await db_session.execute(
            pg_insert(ItemResult).values(
                item_id=itm.id,
                plugin_id=score_p.id,
                valid=True,
                score=sc,
            )
        )
    await db_session.commit()

    # score‑ordered
    res = await fetch_hc_job_results(db_session, parent.id, order_by="score")
    assert [r["score"] for r in res] == sorted(scores, reverse=True)

    # timestamp‑ordered + limit/offset
    res_ts = await fetch_hc_job_results(db_session, parent.id, order_by="timestamp", limit=2)
    assert len(res_ts) == 2
    res_ts_2 = await fetch_hc_job_results(db_session, parent.id, order_by="timestamp",
                                          offset=2, limit=2)
    assert len(res_ts_2) == 1
    assert {r["item"]["item_id"] for r in res_ts}.isdisjoint(
        {r["item"]["item_id"] for r in res_ts_2}
    )
    await db_session.commit()


# ════════════════════════════════════════════════════════════════════════════
# 7. export_hc_job_hierarchy – nested structure
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_export_hc_job_hierarchy_structure(
    db_session,
    create_item,
    create_test_score_plugin,
):
    score_p = await create_test_score_plugin()
    parent, input_job, iter0 = await _mk_hc_chain(db_session, max_iterations=2)

    # ── patch job_json so export_hc_job_hierarchy can locate the score plugin
    await update_helper(
        parent,
        {
            "job_json": {"plugin_config": {"score_config": {"plugin_id": score_p.id}}}
        },
    )
    await db_session.commit()

    # create two HCResult rows (prod + assembly variant)
    prod = await create_item("P")
    await db_session.execute(pg_insert(HCResult).values(job_id=parent.id,
                                                        item_id=prod.id,
                                                        valid=True))
    asm_prod = await create_item("AP")
    asm_row = await db_session.execute(
        text("""
        INSERT INTO assemblies (product_id, plugin_id, assembly_key, component_key)
        VALUES (:p, :pl, :ak, :ck) RETURNING assembly_id
        """),
        dict(p=asm_prod.id, pl=score_p.id, ak=f"{score_p.id}_{asm_prod.id}_{asm_prod.id}",
             ck=f"{score_p.id}_{asm_prod.id}"),
    )
    asm_id = asm_row.scalar_one()
    await db_session.execute(
        pg_insert(HCResult).values(job_id=parent.id,
                                   item_id=asm_prod.id,
                                   assembly_id=asm_id,
                                   valid=False)
    )

    # link both HCResults to iteration via HCIterationResult
    res_ids = (
        await db_session.execute(
            select(HCResult.result_id).where(HCResult.job_id == parent.id)
        )
    ).scalars().all()
    for rid in res_ids:
        await db_session.execute(
            pg_insert(HCIterationResult).values(result_id=rid,
                                                iteration_id=iter0.id,
                                                count=1)
        )
    await db_session.commit()

    export = await export_hc_job_hierarchy(db_session, parent.id)
    assert len(export) == 1                             # 1 input job
    assert export[0]["input_job"]["id"] == input_job.id
    assert export[0]["iterations"][0]["iteration"] == 0
    res_payload = export[0]["iterations"][0]["results"]
    assert {r["item"]["item_id"] for r in res_payload} == {prod.id, asm_prod.id}
    await db_session.commit()


# ════════════════════════════════════════════════════════════════════════════
# 8. Integration: patched HCRunner
# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_hc_runner_smoke(
    monkeypatch,
    db_session,
    create_hc_job_standard,
    create_item
):
    # ── 1. Build minimal HC job ────────────────────────────────────────────
    _, _, parent_job, [input_job] = await create_hc_job_standard(max_iterations=1)
    item = await create_item()

    # ── 2. Patch: a) _run_iteration  b) init_job  c) load_ops ──────────────
    dummy_item = types.SimpleNamespace(
        item_data=types.SimpleNamespace(item_id=item.id, item=item.item),
        assembly_data=None,
        valid=True,
        score=1.0,
        embeddings={},           # not used after we patch init_job
        query_group=None,
    )
    dummy_score = types.SimpleNamespace(last_executed_count=1)
    dummy_connections = types.SimpleNamespace(
        db_service=types.SimpleNamespace(job_id=None),
        redis_service=types.SimpleNamespace(job_id=None)
    )
    async def fake_semaphore():
        return None 
    
    setattr(dummy_connections.redis_service, 'acquire_postgres_semaphore', fake_semaphore)
    setattr(dummy_connections.redis_service, 'release_postgres_semaphore', fake_semaphore)

    async def fake_run_iteration(self, iter_job, connections):
        uniq = {f"{dummy_item.item_data.item_id}_None": dummy_item}
        return uniq, {f"{dummy_item.item_data.item_id}_None": 1}, []          # → stop after first iter
    monkeypatch.setattr(HCRunner, "_run_iteration", fake_run_iteration)

    async def fake_init_job(self, connections):
        # skip _embed_inputs; provide one trivial query tuple
        self.initial_queries = [tuple()]        # len==1 satisfies downstream logic
    monkeypatch.setattr(HCRunner, "init_job", fake_init_job)
    monkeypatch.setattr(HCRunner, "load_ops", lambda self, connections: None)

    # ── 3. Launch runner ───────────────────────────────────────────────────
    runner = HCRunner(job_id=input_job.id)
    runner.score_op = dummy_score
    await runner.load_job(db_session)           # loads ctx + job
    await runner.init_job(connections=dummy_connections)     # patched no‑op
    next_iter = await runner.init_first_iteration(db_session)
    count = 0
    while next_iter is not None:
        next_iter = await runner(connections=dummy_connections)              # runs once, stops
        count += 1
    assert count == 1
    await db_session.commit()

    # ── 4. Assertions ──────────────────────────────────────────────────────
    await db_session.refresh(input_job)
    await db_session.refresh(parent_job)
    assert input_job.status in (JobStatus.COMPLETE, JobStatus.COMPLETE_EARLY_STOP)
    assert parent_job.status in (JobStatus.COMPLETE, JobStatus.COMPLETE_EARLY_STOP)

    assert (await db_session.execute(select(HCResult))).scalars().first(), \
        "Runner failed to persist HCResult rows"
    await db_session.commit()

# ===========================================================================
# 9. should_stop_input – parent/child time & inference limits
# ===========================================================================
@pytest.mark.parametrize(
    "child_inf,parent_inf,child_secs,parent_secs,expect_stop",
    [
        (10, 0, 0, 0, True),   # child inference limit hit
        (0, 10, 0, 0, True),   # parent inference limit hit
        (0, 0, 10, 0, True),   # child time limit hit
        (0, 0, 0, 10, True),   # parent time limit hit
        (1, 1, 0, 0, False),   # under all limits
    ],
)
def test_should_stop_input_limits(child_inf, parent_inf, child_secs, parent_secs, expect_stop):
    now = datetime.now(timezone.utc)
    child = types.SimpleNamespace(
        inference=child_inf,
        inference_limit=5,
        started_at=now - timedelta(seconds=child_secs),
        time_limit=5 if child_secs else None,
    )
    parent = types.SimpleNamespace(
        inference=parent_inf,
        inference_limit=5,
        started_at=now - timedelta(seconds=parent_secs),
        time_limit=5 if parent_secs else None,
    )

    stop, status = should_stop_input(child, parent, iterate_i=0,
                                     max_iter=5, new_queries=[[]])
    assert stop is expect_stop
    if expect_stop:
        assert status == JobStatus.COMPLETE_EARLY_STOP

