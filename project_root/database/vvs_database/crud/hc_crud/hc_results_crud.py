from typing import Dict, List, Tuple, Optional, Any, Literal 
from sqlalchemy.orm import joinedload, aliased
from sqlalchemy import (
    select, and_, func, desc, asc, nulls_last
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.models import (
    HCResult, 
    HCIterationResult, 
    HCIterationJob, 
    HCInputJob,
    HCJob,
    HCInputItems,
    Assembly,
    AssemblyComponent,
    ItemResult,
    Plugin,
    JobPlugin
)
from vvs_database.schemas.internal_schemas import InternalItem

async def upsert_hc_results(
    db: AsyncSession,
    job_id: int,
    items: List[InternalItem],
) -> Dict[Tuple[int, int], int]:
    """Return {(item_id, assembly_id): result_id} after bulk-upsert."""
    if not items:
        return {}

    # ---------------- prepare rows -----------------
    dedup: dict[tuple[int, int | None], bool] = {}
    for itm in items:
        key = (itm.item_data.item_id,
               getattr(itm.assembly_data, "assembly_id", None))
        # last ‘valid’ flag wins – tweak if you want OR/AND semantics
        dedup[key] = itm.valid

    rows = [
        {
            "job_id": job_id,
            "item_id": k[0],
            "assembly_id": k[1],
            "valid": v,
        }
        for k, v in dedup.items()
    ]

    # split by NULL / NOT-NULL assembly_id because they use different indexes
    with_null, with_value = [], []
    for r in rows:
        (with_null if r["assembly_id"] is None else with_value).append(r)

    id_map: Dict[Tuple[int, int], int] = {}

    # ---------- 1) assembly_id IS NOT NULL -------------
    if with_value:
        stmt = (
            pg_insert(HCResult)
            .values(with_value)
            .on_conflict_do_update(
                index_elements=["job_id", "item_id", "assembly_id"],
                index_where=HCResult.assembly_id.isnot(None),
                set_={"valid": pg_insert(HCResult).excluded.valid},
            )
            .returning(HCResult.result_id,
                      HCResult.item_id,
                      HCResult.assembly_id)
        )
        res = await db.execute(stmt)
        id_map.update({(r.item_id, r.assembly_id): r.result_id for r in res})
        
        

    # ---------- 2) assembly_id IS NULL -----------------
    if with_null:
        stmt = (
            pg_insert(HCResult)
            .values(with_null)
            .on_conflict_do_update(
                index_elements=["job_id", "item_id"],
                index_where=HCResult.assembly_id.is_(None),
                set_={"valid": pg_insert(HCResult).excluded.valid},
            )
            .returning(HCResult.result_id,
                       HCResult.item_id,
                       HCResult.assembly_id)
        )
        res = await db.execute(stmt)
        id_map.update({(r.item_id, None): r.result_id for r in res})

    await db.flush()
    return id_map

async def upsert_hc_iteration_results(
    db: AsyncSession,
    iteration_id: int,
    counts_by_result: Dict[int, int],
) -> None:
    """
    Bulk-upsert into HCIterationResult.  If a (result_id, iteration_id) pair
    already exists, increment its count.
    """
    if not counts_by_result:
        return

    rows = [
        {"result_id": rid, "iteration_id": iteration_id, "count": cnt}
        for rid, cnt in counts_by_result.items()
    ]

    stmt = (
        pg_insert(HCIterationResult)
        .values(rows)
        .on_conflict_do_update(
            index_elements=["result_id", "iteration_id"],
            set_={
                "count": HCIterationResult.count + pg_insert(HCIterationResult).excluded.count
            },
        )
    )

    await db.execute(stmt)
    await db.flush()

async def sum_inference_for_hc_input_job(db: AsyncSession, input_id: int) -> int:
    """Sum inference over all HCIterationJob rows for a given HCInputJob."""
    result = await db.execute(
        select(func.coalesce(func.sum(HCIterationJob.inference), 0))
        .where(HCIterationJob.input_id == input_id)
    )
    return result.scalar_one()

async def sum_inference_for_hc_job(db: AsyncSession, job_id: int) -> int:
    """Sum inference over all HCInputJob rows for a given HCJob."""
    result = await db.execute(
        select(func.coalesce(func.sum(HCInputJob.inference), 0))
        .where(HCInputJob.parent_id == job_id)
    )
    return result.scalar_one()


async def fetch_hc_job_results(
    db: AsyncSession,
    hc_job_id: int,
    *,
    offset: int = 0,
    limit: Optional[int] = 100,
    order_by: Literal["score", "timestamp"] = "score",
) -> List[Dict[str, Any]]:
    """
    Fetch HCResult rows for one HC job—ordered by *score descending*—
    with pagination support.
    Each dict has:
        items: [ {item_id, item_str}, ... ]   (product + components)
        score, score_valid, result_valid,
        assembly_id (or None), created_at
    """

    # ── 1. determine score-plugin id ──────────────────────────────
    hc_job: HCJob = await db.get(HCJob, hc_job_id)
    score_plugin_id: int | None = None

    try:
        score_plugin_id = (
            hc_job.job_json["plugin_config"]["score_config"]["plugin_id"]
        )
    except (TypeError, KeyError):
        pass

    if score_plugin_id is None:
        row = await db.execute(
            select(JobPlugin.plugin_id)
            .join(Plugin, Plugin.id == JobPlugin.plugin_id)
            .where(JobPlugin.job_id == hc_job_id, Plugin.type == "score")
        )
        score_plugin_id = row.scalar_one_or_none()

    if score_plugin_id is None:
        raise ValueError("Cannot determine score-plugin for job %s" % hc_job_id)

    # ── 2. build query ────────────────────────────────────────────
    stmt = (
        select(HCResult)
        .options(
            joinedload(HCResult.item),                      # product Item
            joinedload(HCResult.assembly)
            .selectinload(Assembly.components)              # AssemblyComponent
            .joinedload(AssemblyComponent.component),       # component Item
        )
        .outerjoin(
            ItemResult,
            and_(
                ItemResult.item_id == HCResult.item_id,
                ItemResult.plugin_id == score_plugin_id,
            ),
        )
        .add_columns(
            ItemResult.score.label("item_score"),
            ItemResult.valid.label("score_valid"),
        )
        .where(HCResult.job_id == hc_job_id)
#         .order_by(nulls_last(desc("item_score")))           # score DESC NULLS LAST
        .offset(offset)
    )
    
    if order_by == "score":
        stmt = stmt.order_by(nulls_last(desc("item_score")))
    elif order_by == "timestamp":
        stmt = stmt.order_by(asc(HCResult.created_at))
    else:  # defensive: unreachable if Literal is respected
        raise ValueError("order_by must be 'score' or 'timestamp'")

    if limit is not None:
        stmt = stmt.limit(limit)

    rows = await db.execute(stmt)

    # ── 3. massage into list[dict] ────────────────────────────────
    out: List[Dict[str, Any]] = []

    for hc_res, score, score_valid in rows:
        # gather product + (maybe) component items
        item_dicts = [
#             {"item_id": hc_res.item_id, "item": hc_res.item.item}
        ]

        if hc_res.assembly:
            # Components are already ordered by assembly_index via sort
            comps = sorted(
                hc_res.assembly.components, key=lambda c: c.assembly_index
            )
            item_dicts.extend(
                {
                    "item_id": c.component_id,
                    "item": c.component.item,
                    "assembly_index": c.assembly_index
                }
                for c in comps
            )

        out.append(
            {
                "item": {"item_id": hc_res.item_id, "item": hc_res.item.item},
                "result_valid": hc_res.valid,
                "score": score,
                "score_valid": score_valid,
                "assembly_id": hc_res.assembly_id,
                "assembly_components": item_dicts,
                "created_at": hc_res.created_at,
            }
        )

    return out


async def _determine_score_plugin(db: AsyncSession, hc_job: HCJob) -> int:
    """Return plugin_id of the score plugin for *hc_job* (raises if not found)."""
    try:
        return hc_job.job_json["plugin_config"]["score_config"]["plugin_id"]
    except (TypeError, KeyError):
        pass

    row = await db.execute(
        select(JobPlugin.plugin_id).join(Plugin, Plugin.id == JobPlugin.plugin_id).where(
            JobPlugin.job_id == hc_job.id, Plugin.type == "score"
        )
    )
    plugin_id = row.scalar_one_or_none()
    if plugin_id is None:
        raise ValueError(f"Cannot determine score-plugin for job {hc_job.id}")
    return plugin_id


async def _fetch_iteration_results(
    db: AsyncSession,
    iteration: HCIterationJob,
    score_plugin_id: int,
):
    """Return list of dicts for one iteration sorted by score DESC."""

    # alias because we need HCResult both for loading and for join target
    HCR = aliased(HCResult)

    stmt = (
        select(HCIterationResult)
        .options(
            joinedload(HCIterationResult.result)
            .joinedload(HCResult.item),
            joinedload(HCIterationResult.result)
            .joinedload(HCResult.assembly)
            .selectinload(Assembly.components)
            .joinedload(AssemblyComponent.component),
        )
        .join(                                # ← add this
            HCR, HCR.result_id == HCIterationResult.result_id
        )
        .outerjoin(                           # ← fix condition
            ItemResult,
            and_(
                ItemResult.item_id == HCR.item_id,
                ItemResult.plugin_id == score_plugin_id,
            ),
        )
        .add_columns(
            ItemResult.score.label("item_score"),
            ItemResult.valid.label("score_valid"),
            HCIterationResult.count.label("dup_count"),
        )
        .where(HCIterationResult.iteration_id == iteration.id)
        .order_by(nulls_last(desc("item_score")))
    )

    # stmt = (
    #     select(HCIterationResult)
    #     .options(
    #         joinedload(HCIterationResult.result)
    #         .joinedload(HCResult.item),
    #         joinedload(HCIterationResult.result)
    #         .joinedload(HCResult.assembly)
    #         .selectinload(Assembly.components)
    #         .joinedload(AssemblyComponent.component),
    #     )
    #     .outerjoin(
    #         ItemResult,
    #         and_(
    #             ItemResult.item_id == HCIterationResult.result_id,
    #             ItemResult.plugin_id == score_plugin_id,
    #         ),
    #     )
    #     .add_columns(
    #         ItemResult.score.label("item_score"),
    #         ItemResult.valid.label("score_valid"),
    #         HCIterationResult.count.label("dup_count"),
    #     )
    #     .where(HCIterationResult.iteration_id == iteration.id)
    #     .order_by(nulls_last(desc("item_score")))
    # )

    rows = await db.execute(stmt)

    result_payload = []
    for iter_res, score, score_valid, dup_cnt in rows:
        hc_res = iter_res.result
        components_info = []
        if hc_res.assembly:
            comps = sorted(hc_res.assembly.components, key=lambda c: c.assembly_index)
            components_info = [
                {
                    "item_id": c.component_id,
                    "item": c.component.item,
                    "assembly_index": c.assembly_index,
                }
                for c in comps
            ]
        result_payload.append(
            {
                "item": {"item_id": hc_res.item_id, "item": hc_res.item.item},
                "result_valid": hc_res.valid,
                "score": score,
                "score_valid": score_valid,
                "assembly_id": hc_res.assembly_id,
                "assembly_components": components_info,
                "created_at": hc_res.created_at,
                "count": dup_cnt,
            }
        )
    return result_payload


async def export_hc_job_hierarchy(
    db: AsyncSession,
    hc_job_id: int,
) -> List[Dict[str, Any]]:
    """Exports Hill-Climb job results for one *HCJob* in a nested structure:
    HCInputJob → Iteration → Results (score-sorted).

    Return type (informal):
    [List[Dict]] :: one item per HCInputJob
        {
            "input_job": {
                "id": int,
                "max_iterations": int,
                "inference": int,
                "input_items": [
                    {"item_id": int, "item": str, "assembly_index": int, "external_id": str | None},
                    ...
                ],
            },
            "iterations": [
                {
                    "iteration": int,
                    "iteration_id": int,
                    "inference": int | None,
                    "created_at": datetime,
                    "results": [
                        {
                            "item": {"item_id": int, "item": str},
                            "result_valid": bool,
                            "score": float | None,
                            "score_valid": bool | None,
                            "assembly_id": int | None,
                            "assembly_components": [
                                {"item_id": int, "item": str, "assembly_index": int},
                                ...
                            ],
                            "created_at": datetime,
                            "count": int,          # from HCIterationResult.count
                        },
                        ... (score-sorted)
                    ],
                },
                ... (iteration-sorted)
            ],
        }
    """

    hc_job: HCJob = await db.get(HCJob, hc_job_id)
    if hc_job is None:
        raise ValueError(f"HCJob {hc_job_id} not found")

    score_plugin_id = await _determine_score_plugin(db, hc_job)

    # ------------------------------------------------------------------
    # 1) Fetch all input jobs + their items + iterations eager‑loaded
    # ------------------------------------------------------------------
    stmt_inputs = (
        select(HCInputJob)
        .where(HCInputJob.parent_id == hc_job_id)
        .options(
            joinedload(HCInputJob.input_items).joinedload(HCInputItems.item),
            joinedload(HCInputJob.iterations),
        )
        .order_by(HCInputJob.id)
    )
    result_inputs = await db.execute(stmt_inputs)
    input_jobs: List[HCInputJob] = result_inputs.scalars().unique().all()

    export_data: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 2) For each input job build nested structure
    # ------------------------------------------------------------------
    for inp in input_jobs:
        # Input‑level dict
        input_dict: Dict[str, Any] = {
            "input_job": {
                "id": inp.id,
                "max_iterations": inp.max_iterations,
                "inference": inp.inference,
                "status": inp.status,
                "input_items": [
                    {
                        "item_id": itm.item_id,
                        "item": itm.item.item,
                        "assembly_index": itm.assembly_index,
                        "external_id": itm.external_id,
                    }
                    for itm in sorted(inp.input_items, key=lambda it: it.assembly_index)
                ],
            },
            "iterations": [],
        }

        # Iterations sorted by iteration number
        iterations_sorted = sorted(inp.iterations, key=lambda it: it.iteration)

        for it_job in iterations_sorted:
            iteration_results = await _fetch_iteration_results(db, it_job, score_plugin_id)
            input_dict["iterations"].append(
                {
                    "iteration": it_job.iteration,
                    "iteration_id": it_job.id,
                    "inference": it_job.inference,
                    "created_at": it_job.created_at if hasattr(it_job, "created_at") else None,
                    "results": iteration_results,
                }
            )

        export_data.append(input_dict)

    return export_data
