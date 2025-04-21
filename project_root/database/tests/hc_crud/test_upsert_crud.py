import types

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from vvs_database.models.job_models.hc_models import (
    HCResult,
    HCIterationResult,
)
from vvs_database.models.job_models.job_models import JobType
from vvs_database.crud.hc_crud.hc_results_crud import (
    upsert_hc_results,
    upsert_hc_iteration_results,
)
from vvs_database.crud import create_job, assembly_checkin
from vvs_database.schemas import NewAssembly 

# ────────────────────────────────────────────────────────────────────────────
# Tiny stand-ins for InternalItem / AssemblyData (same as earlier)
# ────────────────────────────────────────────────────────────────────────────
def _mk_item(item_id: int, item_str: str, *, valid: bool = True):
    item_data = types.SimpleNamespace(item_id=item_id, item=item_str)
    return types.SimpleNamespace(item_data=item_data, assembly_data=None, valid=valid)


def _attach_assembly(internal_item, assembly_id: int):
    internal_item.assembly_data = types.SimpleNamespace(assembly_id=assembly_id)
    return internal_item


async def _mk_hc_job_chain(db):
    """
    Build the minimum HCJob → HCInputJob → HCIterationJob chain and return
    (parent_job, input_job, iter_job).
    """
    parent_job = await create_job(
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
        extra_args=dict(parent_id=parent_job.id, max_iterations=1),
    )

    iter_job = await create_job(
        db,
        job_type=JobType.HILL_CLIMB_JOB_ITERATION,
        job_json=None,
        auto_execute=False,
        extra_args=dict(input_id=input_job.id, iteration=0),
    )

    return parent_job, input_job, iter_job


# ────────────────────────────────────────────────────────────────────────────
# 1. upsert_hc_results – insert + update, plain vs assembly
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_upsert_hc_results_insert_then_update(
    db_session,
    create_item,
    create_test_assembly_plugin,  # uses assembly_checkin helper indirectly
):
    # Items + assembly -------------------------------------------------------
    prod_plain = await create_item("prod-plain")
    prod_asm   = await create_item("prod-asm")
    comp       = await create_item("component-X")

    asm_plugin = await create_test_assembly_plugin()
    asm_dict = [
        NewAssembly(**{
            "item": prod_asm.item,
            "external_id": None,
            "components": [{"item_id": comp.id, "assembly_index": 0}],
        })
    ]

    assembly_result = await assembly_checkin(db_session, asm_dict, asm_plugin.id)
    assembly_id = assembly_result["assemblies"][0].assembly_id

    # HC-job hierarchy -------------------------------------------------------
    parent_job, *_ = await _mk_hc_job_chain(db_session)

    # InternalItem mocks
    item_plain = _mk_item(prod_plain.id, prod_plain.item, valid=True)
    item_asm   = _attach_assembly(_mk_item(prod_asm.id, prod_asm.item, valid=False), assembly_id)

    # -- first upsert (insert) ----------------------------------------------
    id_map_1 = await upsert_hc_results(
        db_session,
        job_id=parent_job.id,
        items=[item_plain, item_asm],
    )
    rows = (await db_session.execute(select(HCResult).filter(HCResult.job_id==parent_job.id))).scalars().all()
    assert len(rows) == 2

    # -- second upsert (toggle valid flag) ----------------------------------
    item_plain.valid = False
    item_asm.valid   = True
    id_map_2 = await upsert_hc_results(
        db_session,
        job_id=parent_job.id,
        items=[item_plain, item_asm],
    )
    assert id_map_1 == id_map_2  # same PKs ↔ no duplicates

    # verify flags updated
    plain_valid = (
        await db_session.execute(
            select(HCResult.valid).where(HCResult.item_id == prod_plain.id)
        )
    ).scalar_one()
    asm_valid = (
        await db_session.execute(
            select(HCResult.valid).where(
                HCResult.item_id == prod_asm.id, HCResult.assembly_id == assembly_id
            )
        )
    ).scalar_one()
    assert plain_valid is False
    assert asm_valid is True
    await db_session.commit()


# ────────────────────────────────────────────────────────────────────────────
# 2. upsert_hc_iteration_results – counts accumulate
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_upsert_hc_iteration_results_accumulates(
    db_session,
    create_item,
):
    # HC-job hierarchy -------------------------------------------------------
    parent_job, input_job, iter_job = await _mk_hc_job_chain(db_session)

    # 2 dummy HCResult rows so we have valid result_ids ----------------------
    dummy_items = [await create_item(f"dummy {i}") for i in range(2)]
    res_ids = []
    for itm in dummy_items:
        rec = await db_session.execute(
            pg_insert(HCResult)
            .values(
                job_id=parent_job.id,
                item_id=itm.id,
                assembly_id=None,
                valid=True,
            )
            .returning(HCResult.result_id)
        )
        res_ids.append(rec.scalar_one())

    # 1st upsert (1,3) -------------------------------------------------------
    await upsert_hc_iteration_results(
        db_session,
        iteration_id=iter_job.id,
        counts_by_result={res_ids[0]: 1, res_ids[1]: 3},
    )
    # 2nd upsert (+2,+2) -----------------------------------------------------
    await upsert_hc_iteration_results(
        db_session,
        iteration_id=iter_job.id,
        counts_by_result={res_ids[0]: 2, res_ids[1]: 2},
    )

    # final counts should be 3 and 5
    rows = (
        await db_session.execute(
            select(HCIterationResult.result_id, HCIterationResult.count).where(
                HCIterationResult.iteration_id == iter_job.id
            )
        )
    ).all()
    counts = {rid: cnt for rid, cnt in rows}
    assert counts[res_ids[0]] == 3
    assert counts[res_ids[1]] == 5
    await db_session.commit()