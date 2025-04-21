import types
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ORM & helpers -------------------------------------------------------------
from vvs_database.models import (
    Item,
    HCResult,
    HCIterationResult,
    HCInputJob,
    JobPlugin,
    Plugin,
    ItemResult,
    Job
)
from vvs_database.schemas import JobStatus, JobType 

from vvs_database.crud.hc_crud.hc_results_crud import (
    upsert_hc_results,
    upsert_hc_iteration_results,
    fetch_hc_job_results,
    export_hc_job_hierarchy,
)
from vvs_database.crud.hc_crud.hc_job_crud import latest_hc_iteration
from vvs_database.crud.job_crud import create_job, update_helper
from vvs_database.crud.hc_crud.hc_results_crud import fetch_hc_job_results
from vvs_database.crud.hc_crud.hc_results_crud import export_hc_job_hierarchy
from vvs_database.job_runner.hc_runner.hc_utils import should_stop_input

# ===========================================================================
# Helper: minimal HCJob + optional iteration
# ===========================================================================
async def _mk_parent_and_input(db, with_iter: bool = True):
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
        extra_args=dict(parent_id=parent.id, max_iterations=1),
    )
    iter_job = None
    if with_iter:
        iter_job = await create_job(
            db,
            job_type=JobType.HILL_CLIMB_JOB_ITERATION,
            job_json=None,
            auto_execute=False,
            extra_args=dict(input_id=input_job.id, iteration=0),
        )
    return parent, input_job, iter_job


# ===========================================================================
# 1. upsert_hc_results duplicates in SAME batch
# ===========================================================================
@pytest.mark.asyncio
async def test_upsert_hc_results_same_batch_dedup(db_session, create_item):
    parent, *_ = await _mk_parent_and_input(db_session)
    itm = await create_item("dup-test")

    # two identical InternalItem mocks (same key) in one list
    mock = types.SimpleNamespace(
        item_data=types.SimpleNamespace(item_id=itm.id, item=itm.item),
        assembly_data=None,
        valid=True,
    )
    id_map = await upsert_hc_results(db_session, parent.id, [mock, mock])
    # exactly one row written
    rows = (await db_session.execute(select(HCResult))).scalars().all()
    assert len(rows) == 1
    # and id_map has one entry
    assert len(id_map) == 1
    await db_session.commit()


# ===========================================================================
# 2. upsert_hc_iteration_results: zero‑count is a no‑op
# ===========================================================================
@pytest.mark.asyncio
async def test_upsert_hc_iteration_results_zero_noop(db_session, create_item):
    parent, *_ , iter_job = await _mk_parent_and_input(db_session, with_iter=True)
    itm = await create_item("count-test")
    res_id = (
        await db_session.execute(
            pg_insert(HCResult)
            .values(job_id=parent.id, item_id=itm.id, valid=True)
            .returning(HCResult.result_id)
        )
    ).scalar_one()

    # initial count = 2
    await upsert_hc_iteration_results(db_session, iter_job.id, {res_id: 2})
    # second call with 0 → should leave count untouched
    await upsert_hc_iteration_results(db_session, iter_job.id, {res_id: 0})

    cnt = (
        await db_session.execute(
            select(HCIterationResult.count).where(
                HCIterationResult.result_id == res_id, HCIterationResult.iteration_id == iter_job.id
            )
        )
    ).scalar_one()
    assert cnt == 2
    await db_session.commit()


# ===========================================================================
# 3. _determine_score_plugin fallback via JobPlugin rows
# ===========================================================================
@pytest.mark.asyncio
async def test_determine_score_plugin_fallback(db_session,
                                               create_item,
                                               create_test_score_plugin):
    # score plugin & job chain
    score_p = await create_test_score_plugin()
    parent, *_ = await _mk_parent_and_input(db_session, with_iter=False)

    # wire the plugin via JobPlugin (no job_json entry)
    await db_session.execute(
        pg_insert(JobPlugin).values(job_id=parent.id, plugin_id=score_p.id)
    )

    # one HCResult + ItemResult with score 0.42
    prod = await create_item("scored-item")
    await db_session.execute(
        pg_insert(HCResult).values(job_id=parent.id, item_id=prod.id, valid=True)
    )
    await db_session.execute(
        pg_insert(ItemResult).values(
            item_id=prod.id, plugin_id=score_p.id, score=0.42, valid=True
        )
    )
    await db_session.commit()

    rows = await fetch_hc_job_results(db_session, parent.id)
    assert rows and rows[0]["score"] == 0.42
    await db_session.commit()


# ===========================================================================
# 4. fetch_hc_job_results offset==len(rows) returns []
# ===========================================================================
@pytest.mark.asyncio
async def test_fetch_hc_job_results_pagination_boundary(db_session,
                                                        create_item,
                                                        create_test_score_plugin):
    score_p = await create_test_score_plugin()
    parent, *_ = await _mk_parent_and_input(db_session, with_iter=False)
    await update_helper(parent, {"job_json": {"plugin_config": {"score_config": {"plugin_id": score_p.id}}}})

    for i in range(3):
        itm = await create_item(f"pag{i}")
        await db_session.execute(pg_insert(HCResult).values(job_id=parent.id, item_id=itm.id, valid=True))
        await db_session.execute(pg_insert(ItemResult).values(item_id=itm.id, plugin_id=score_p.id, score=i, valid=True))
    await db_session.commit()

    rows = await fetch_hc_job_results(db_session, parent.id)
    empty = await fetch_hc_job_results(db_session, parent.id, offset=len(rows))
    assert rows and empty == []
    await db_session.commit()


# ===========================================================================
# 5. cleanup_unreferenced for Item & Job
# ===========================================================================
@pytest.mark.asyncio
async def test_cleanup_unreferenced(db_session, create_item):
    # orphan item
    orphan_item = await create_item("orphan")
    # orphan job
    orphan_job = await create_job(db_session, job_type=JobType.TEST_JOB)

    di = await Item.cleanup_unreferenced(db_session)
    dj = await Job.cleanup_unreferenced(db_session)

    assert di >= 1 and dj >= 1
    await db_session.commit()


# ===========================================================================
# 6. latest_hc_iteration on empty returns None
# ===========================================================================
@pytest.mark.asyncio
async def test_latest_hc_iteration_empty(db_session):
    _, input_job, _ = await _mk_parent_and_input(db_session, with_iter=False)
    assert await latest_hc_iteration(db_session, input_job.id) is None
    await db_session.commit()


# ===========================================================================
# 7. should_stop_input early‑stop (time, child inference, parent inference)
# ===========================================================================
@pytest.mark.parametrize("hit_time, child_inf, parent_inf", [
    (True, 0, 0),   # time‑limit
    (False, 10, 0), # child inference‑limit
    (False, 0, 10), # parent inference‑limit
])
def test_should_stop_input_early(hit_time, child_inf, parent_inf):
    now = datetime.now(timezone.utc)
    child = types.SimpleNamespace(
        inference=child_inf,
        inference_limit=5,
        started_at=now - timedelta(seconds=10) if hit_time else now,
        time_limit=5 if hit_time else None,
    )
    parent = types.SimpleNamespace(
        inference=parent_inf,
        inference_limit=5,
        started_at=now,
        time_limit=None,
    )
    stop, status = should_stop_input(child, parent, iterate_i=0,
                                     max_iter=5, new_queries=[[]])
    assert stop and status == JobStatus.COMPLETE_EARLY_STOP


# ===========================================================================
# 8. export_hc_job_hierarchy aggregates counts
# ===========================================================================
@pytest.mark.asyncio
async def test_export_hierarchy_aggregates_counts(db_session,
                                                  create_item,
                                                  create_test_score_plugin):
    score_p = await create_test_score_plugin()
    parent, input_job, iter0 = await _mk_parent_and_input(db_session, with_iter=True)
    await update_helper(parent, {"job_json": {"plugin_config": {"score_config": {"plugin_id": score_p.id}}}})

    # HCResult row
    prod = await create_item("agg-prod")
    res_id = (
        await db_session.execute(
            pg_insert(HCResult).values(job_id=parent.id, item_id=prod.id, valid=True).returning(HCResult.result_id)
        )
    ).scalar_one()

    # upsert iteration counts twice → total 3
    await upsert_hc_iteration_results(db_session, iter0.id, {res_id: 1})
    await upsert_hc_iteration_results(db_session, iter0.id, {res_id: 2})
    await db_session.commit()

    export = await export_hc_job_hierarchy(db_session, parent.id)
    cnt = export[0]["iterations"][0]["results"][0]["count"]
    assert cnt == 3
    await db_session.commit()
