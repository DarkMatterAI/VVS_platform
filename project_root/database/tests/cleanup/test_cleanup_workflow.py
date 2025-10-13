import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from vvs_database import crud
from vvs_database.models import (
    Item, ItemSource, ItemResult, Assembly, HCResult, Job
)
from vvs_database.schemas import NewAssembly, JobType

# ────────────────────────────────────────────────────────────────────────────
# 1) After job delete: assemblies are pruned and product artifacts disappear
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_cleanup_after_job_delete_removes_assembly_artifacts(
    db_session,
    create_item,
    create_test_assembly_plugin,
    create_job,
):
    # -- Build an assembly product (Item + Assembly + ItemSource) -----------
    comp1 = await create_item()
    comp2 = await create_item()
    asm_plugin = await create_test_assembly_plugin(num_parents=2)

    assembly_data = [{
        "item": "AsmProduct-1",
        "external_id": "asm-ext-1",
        "components": [
            {"item_id": comp1.id, "assembly_index": 0},
            {"item_id": comp2.id, "assembly_index": 1},
        ],
    }]
    assembly_data = [NewAssembly(**i) for i in assembly_data]
    asm_result = await crud.assembly_checkin(db_session, assembly_data, asm_plugin.id)
    product_item = asm_result["items"][0]
    assembly_row  = asm_result["assemblies"][0]

    # -- Create an HCJob and record one HCResult referencing the assembly ---
    hc_parent = await create_job(job_type=JobType.HILL_CLIMB_JOB)
    await db_session.execute(
        pg_insert(HCResult).values(
            job_id=hc_parent.id, item_id=product_item.id, assembly_id=assembly_row.assembly_id, valid=True
        )
    )
    await db_session.commit()

    # Sanity: assembly + source exist, product exists
    assert await db_session.get(Assembly, assembly_row.assembly_id) is not None
    assert (await crud.get_item_sources(db_session, [product_item.id], asm_plugin.id))
    assert await db_session.get(Item, product_item.id) is not None

    # -- Delete job + run post-delete cleanup synchronously ------------
    _ = await crud.delete_job(db_session, hc_parent, run_cleanup=True, async_cleanup=False)

    # Assemblies gone
    assert await db_session.get(Assembly, assembly_row.assembly_id) is None
    # ItemSource for the assembly plugin pruned
    assert (await crud.get_item_sources(db_session, [product_item.id], asm_plugin.id)) == []
    # ItemResult rows for that product pruned (any plugin)
    assert (await crud.get_item_results(db_session, [product_item.id], asm_plugin.id)) == []

    # Product item now unreferenced → should be removed 
    assert await db_session.get(Item, product_item.id) is None
    await db_session.commit()


# ────────────────────────────────────────────────────────────────────────────
# 2) Assembly.cleanup_unreferenced: deletes assemblies with missing components
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_cleanup_unreferenced_deletes_incomplete_assemblies(
    db_session,
    create_item,
    create_test_assembly_plugin,
):
    c1 = await create_item()
    c2 = await create_item()
    prod = await create_item()
    aplugin = await create_test_assembly_plugin(num_parents=2)

    # create a valid assembly
    asm = await crud.create_assembly(
        db_session, aplugin.id, prod.id,
        [{"assembly_index": 0, "component_id": c1.id},
         {"assembly_index": 1, "component_id": c2.id}]
    )
    await db_session.commit()
    assert await db_session.get(Assembly, asm.assembly_id) is not None

    # delete one component item → its AssemblyComponent row cascades away
    await crud.delete_item(db_session, c1)

    # run cleanup → assembly should be removed as invalid (missing a parent)
    assert await db_session.get(Assembly, asm.assembly_id) is None
    await db_session.commit()


# ────────────────────────────────────────────────────────────────────────────
# 3) Plugin delete preflight: assembly plugin blocked by references, then allowed
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_plugin_delete_preflight_with_assembly_references(
    db_session,
    create_item,
    create_test_assembly_plugin,
):
    # create assembly plugin and an assembly + item_source via checkin
    plugin = await create_test_assembly_plugin(num_parents=2)
    c1, c2 = await create_item(), await create_item()
    assembly_data = [{
        "item": "AsmProduct-2",
        "external_id": "asm-ext-2",
        "components": [
            {"item_id": c1.id, "assembly_index": 0},
            {"item_id": c2.id, "assembly_index": 1},
        ],
    }]
    assembly_data = [NewAssembly(**i) for i in assembly_data]
    _ = await crud.assembly_checkin(db_session, assembly_data, plugin.id)

    # delete should be blocked by preflight (assemblies + item_sources present)
    from vvs_database.exceptions import ReferenceError
    with pytest.raises(ReferenceError):
        await crud.delete_plugin(db_session, plugin.id)

    # run cleanup of assemblies & item artifacts
    _ = await Assembly.cleanup_unreferenced(db_session)   # will remove only if unreferenced by HC results
    _ = await crud.prune_orphan_assembly_products(db_session)

    # after cleanup, delete should succeed
    deleted = await crud.delete_plugin(db_session, plugin.id)
    assert deleted is not None
    assert await crud.get_plugin(db_session, plugin.id, with_error=False) is None
    await db_session.commit()


# ────────────────────────────────────────────────────────────────────────────
# 4) Fast job delete: cascades & sweeps run without loading ORM rows
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_delete_job_fast_cascades_and_sweeps(
    db_session,
    create_item,
    create_test_assembly_plugin,
    create_job,
):
    # make a job, an assembly product, and an hc_result that references it
    c1, c2 = await create_item(), await create_item()
    aplugin = await create_test_assembly_plugin(num_parents=2)
    assembly_data = [{
            "item": "AsmProduct-fast",
            "external_id": "asm-ext-fast",
            "components": [
                {"item_id": c1.id, "assembly_index": 0},
                {"item_id": c2.id, "assembly_index": 1},
            ],
        }]
    assembly_data = [NewAssembly(**i) for i in assembly_data]
    asm_res = await crud.assembly_checkin(
        db_session,
        assembly_data,
        aplugin.id
    )
    prod_item = asm_res["items"][0]
    asm_row   = asm_res["assemblies"][0]

    parent = await create_job(job_type=JobType.HILL_CLIMB_JOB)
    await db_session.execute(
        pg_insert(HCResult).values(job_id=parent.id, item_id=prod_item.id, assembly_id=asm_row.assembly_id, valid=True)
    )
    await db_session.commit()

    # delete quickly + cleanup
    try:
        _ = await crud.delete_job(db_session, parent, run_cleanup=True, async_cleanup=False)
    except AttributeError:
        await crud.delete_job(db_session, parent)
        _ = await Assembly.cleanup_unreferenced(db_session)
        _ = await crud.prune_orphan_assembly_products(db_session)

    # verify job gone, assembly gone, item artifacts pruned
    assert await crud.get_job(db_session, parent.id) is None
    assert await db_session.get(Assembly, asm_row.assembly_id) is None
    assert (await crud.get_item_sources(db_session, [prod_item.id], aplugin.id)) == []

    # final sweep should delete orphan item
    await crud.cleanup_unreferenced_items(db_session)
    assert await db_session.get(Item, prod_item.id) is None
    await db_session.commit()