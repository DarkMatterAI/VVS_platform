from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload
import asyncio 
from typing import List, Dict, Optional 

from vvs_database.models import (
    Item, 
    ItemSource, 
    ItemResult, 
    Assembly, 
    AssemblyComponent, 
    PluginExecutionFailure
)
from vvs_database import schemas
from vvs_database.utils import chunked, with_deadlock_retry, LOCK_NS, with_lock_and_retry

async def _upsert_items_single_batch(
    db: AsyncSession, payload: list[str]
) -> list[Item]:
    """Single SQL round-trip (insert-ignore + select missing)."""
    ins = (
        pg_insert(Item)
        .values([{"item": s} for s in payload])
        .on_conflict_do_nothing(index_elements=["item"])
        .returning(Item.id, Item.item, Item.created_at)
    )
    rows = (await db.execute(ins)).fetchall()
    row_map = {r.item: r for r in rows}

    missing = [s for s in payload if s not in row_map]
    if missing:
        extra = (
            await db.execute(
                select(Item.id, Item.item, Item.created_at).where(Item.item.in_(missing))
            )
        ).fetchall()
        row_map.update({r.item: r for r in extra})

    return [row_map[s] for s in payload]


async def upsert_items(
    db: AsyncSession,
    new_items: list[dict],
    *,
    batch_size: int = 500,
) -> list[Item]:
    """Dead-lock resilient, batched version."""
    if not new_items:
        return []

    # step 1 – build list[string] in original order (allow duplicates)
    ordered_items = [d["item"] for d in new_items]

    # step 2 – unique values per batch
    batches = chunked(ordered_items, batch_size)
    results: list[Item] = []

    for batch_strings in batches:
        unique_payload = list({s for s in batch_strings})
        batch_rows = await with_lock_and_retry(
            db,
            LOCK_NS["items"],
            lambda: _upsert_items_single_batch(db, unique_payload)
        )
        # map back to original order (duplicates kept)
        row_map = {r.item: r for r in batch_rows}
        results.extend(row_map[s] for s in batch_strings)
        await db.commit()

    return results

async def _upsert_item_sources_single(db: AsyncSession, payload: list[dict]):
    stmt = (
        pg_insert(ItemSource)
        .values(payload)
        .on_conflict_do_update(
            index_elements=["item_id", "plugin_id"],
            set_={"external_id": pg_insert(ItemSource).excluded.external_id},
        )
        .returning(
            ItemSource.item_id,
            ItemSource.external_id,
            ItemSource.plugin_id,
            ItemSource.created_at,
        )
    )
    return (await db.execute(stmt)).fetchall()

async def upsert_item_sources(
    db: AsyncSession,
    new_item_sources: list[dict],
    *,
    batch_size: int = 500,
) -> list:
    if not new_item_sources:
        return []

    out = []
    for chunk in chunked(new_item_sources, batch_size):
        rows = await with_lock_and_retry(
            db,
            LOCK_NS["item_sources"],
            lambda: _upsert_item_sources_single(db, chunk)
        )
        out.extend(rows)
        await db.commit()
    return out

async def item_checkin(
    db: AsyncSession,
    new_items: list[schemas.NewItem],
    plugin_id: int | None,
    *,
    batch_size: int = 500,
) -> dict:
    # ---- items -----------------------------------------------------------
    uniq_items = {ni.item: ni.external_id for ni in new_items}
    item_rows  = await upsert_items(
        db,
        [{"item": s} for s in uniq_items],
        batch_size=batch_size,
    )
    row_map = {r.item: r for r in item_rows}
    items_out = [row_map[ni.item] for ni in new_items]

    # ---- item_sources ----------------------------------------------------
    sources_out = []
    if plugin_id is not None:
        src_payload = [
            {
                "item_id": row_map[item].id,
                "external_id": (ext if ext is None else str(ext)),
                "plugin_id": plugin_id,
            }
            for item, ext in uniq_items.items()
        ]
        src_rows = await upsert_item_sources(db, src_payload, batch_size=batch_size)
        src_map  = {r.item_id: r for r in src_rows}
        sources_out = [src_map[i.id] for i in items_out]

    await db.commit()
    return {"items": items_out, "item_sources": sources_out}

async def _upsert_item_results_single(
    db: AsyncSession,
    payload: list[dict],          # unique item‑ids for this batch
    plugin_id: int,
):
    stmt = (
        pg_insert(ItemResult)
        .values(payload)
        .on_conflict_do_update(
            index_elements=[ItemResult.item_id, ItemResult.plugin_id],
            set_={
                "valid":      pg_insert(ItemResult).excluded.valid,
                "score":      pg_insert(ItemResult).excluded.score,
                "embedding":  pg_insert(ItemResult).excluded.embedding,
                "created_at": func.now(),
            },
        )
        .returning(
            ItemResult.item_id,
            ItemResult.plugin_id,
            ItemResult.valid,
            ItemResult.score,
            ItemResult.embedding,
            ItemResult.created_at,
        )
    )
    return (await db.execute(stmt)).fetchall()

async def result_checkin(
    db: AsyncSession,
    new_results: list[schemas.NewResult],
    plugin_id: int,
    *,
    batch_size: int = 500,
) -> list[ItemResult]:
    """
    Upsert many ItemResult rows, retrying automatically on dead-locks.

    Returns one ItemResult per *input element* preserving order (duplicates
    included).
    """
    if not new_results:
        return []

    # final output we’ll fill in input order
    out: list[ItemResult] = [None] * len(new_results)   # type: ignore

    # iterate by *input* batches (duplicates stay together)
    for batch_idx, batch in enumerate(chunked(list(enumerate(new_results)), batch_size)):
        # 1) build payload –  one value per *unique* item_id in this batch
        uniq: dict[int, schemas.NewResult] = {}
        for pos, rec in batch:      # preserve last‑seen => mirrors old logic
            uniq[rec.item_id] = rec

        values = [
            {
                "item_id":   rec.item_id,
                "plugin_id": plugin_id,
                "valid":     rec.valid,
                "score":     rec.score,
                "embedding": rec.embedding,
            }
            for rec in uniq.values()
        ]

        # 2) run the SQL with dead‑lock retry
        lock_id = LOCK_NS["item_results"] * 1_000_000 + plugin_id
        rows = await with_lock_and_retry(
            db,
            lock_id,
            lambda: _upsert_item_results_single(db, values, plugin_id)
        )
        row_map = {r.item_id: r for r in rows}

        # 3) copy back to *out* preserving duplicates & original order
        for pos, rec in batch:
            out[pos] = row_map[rec.item_id]
        await db.commit()

    return out      # list[ItemResult] in same order/length as input

async def _insert_assemblies_single(
    db: AsyncSession, payload: list[dict]
) -> list[tuple[int, str]]:         # [(assembly_id, assembly_key)]
    if not payload:
        return []
    stmt = (
        pg_insert(Assembly)
        .values(payload)
        .on_conflict_do_nothing(index_elements=["assembly_key"])
        .returning(Assembly.assembly_id, Assembly.assembly_key)
    )
    return (await db.execute(stmt)).fetchall()


async def _insert_components_single(db: AsyncSession, payload: list[dict]):
    if not payload:
        return
    stmt = (
        pg_insert(AssemblyComponent)
        .values(payload)
        .on_conflict_do_nothing(
            index_elements=["assembly_id", "assembly_index"]
        )
    )
    await db.execute(stmt)

async def assembly_checkin(
    db: AsyncSession,
    new_assemblies: list[schemas.NewAssembly],
    plugin_id: int,
    *,
    batch_size: int = 200,
) -> dict:
    """
    Bulk, dead-lock-resilient check-in for assemblies + their components.
    Returns dict with same shape as before.
    """
    if not new_assemblies:
        return {"items": [], "item_sources": [], "assemblies": []}

    # ----------------------------------------------------------------------
    # 0) Upsert all *product* + *component* items first  (re‑uses item_checkin)
    # ----------------------------------------------------------------------
    item_ci = await item_checkin(db, new_assemblies, plugin_id, batch_size=batch_size)
    product_rows = item_ci["items"]
    prod_map = {r.item: r.id for r in product_rows}  # str → id

    # ----------------------------------------------------------------------
    # 1) Build assembly payloads
    # ----------------------------------------------------------------------
    asm_payload: dict[str, dict] = {}          # assembly_key → row‑dict
    comp_payload_by_asm: dict[str, list[dict]] = {}
    asm_keys_in_order: list[str] = []

    for asm in new_assemblies:
        prod_id = prod_map[asm.item]
        sorted_comps = sorted(asm.components, key=lambda c: c.assembly_index)
        comp_ids = [c.item_id for c in sorted_comps]

        asm_key  = f"{plugin_id}_{'_'.join(map(str, comp_ids))}_{prod_id}"
        comp_key = f"{plugin_id}_{'_'.join(map(str, comp_ids))}"

        asm_keys_in_order.append(asm_key)

        asm_payload.setdefault(
            asm_key,
            {
                "product_id":   prod_id,
                "plugin_id":    plugin_id,
                "assembly_key": asm_key,
                "component_key": comp_key,
            },
        )

        comp_payload_by_asm.setdefault(asm_key, []).extend(
            {
                "assembly_index": c.assembly_index,
                "component_id":   c.item_id,
            }
            for c in sorted_comps
        )

    # ----------------------------------------------------------------------
    # 2) Insert assemblies in batches (dead‑lock retry)
    # ----------------------------------------------------------------------
    asm_id_map: dict[str, int] = {}  # assembly_key → assembly_id

    # first, fetch any that already exist (one query only)
    existing = (
        await db.execute(
            select(Assembly.assembly_id, Assembly.assembly_key).where(
                Assembly.assembly_key.in_(asm_payload)
            )
        )
    ).fetchall()
    asm_id_map.update({k: i for i, k in existing})

    # then insert *only* the missing ones batch‑wise
    missing_rows = [
        asm_payload[k] for k in asm_payload.keys() if k not in asm_id_map
    ]
    for chunk in chunked(missing_rows, batch_size):
        rows = await with_lock_and_retry(
            db,
            LOCK_NS["assemblies"],
            lambda: _insert_assemblies_single(db, chunk)
        )
        asm_id_map.update({k: i for i, k in rows})
        await db.commit()

    # ----------------------------------------------------------------------
    # 3) Insert components for the assemblies we just created
    # ----------------------------------------------------------------------
    comp_rows: list[dict] = []
    for asm_key, asm_id in asm_id_map.items():
        for comp in comp_payload_by_asm[asm_key]:
            comp_rows.append(
                {
                    "assembly_id":    asm_id,
                    "assembly_index": comp["assembly_index"],
                    "component_id":   comp["component_id"],
                }
            )
    for chunk in chunked(comp_rows, batch_size):
        await with_lock_and_retry(
            db,
            LOCK_NS["components"],
            lambda: _insert_components_single(db, chunk)
        )
        await db.commit()

    # ----------------------------------------------------------------------
    # 4) Load all assemblies with components for the response
    # ----------------------------------------------------------------------
    assemblies = (
        await db.execute(
            select(Assembly)
            .options(selectinload(Assembly.components))
            .where(Assembly.assembly_key.in_(asm_keys_in_order))
        )
    ).scalars().all()
    asm_map = {a.assembly_key: a for a in assemblies}

    await db.commit()

    return {
        "items":        product_rows,
        "item_sources": item_ci["item_sources"],
        "assemblies":   [asm_map[k] for k in asm_keys_in_order],
    }

async def upsert_execution_failures(db: AsyncSession, records: List[Dict]):
    ins_stmt_source = pg_insert(PluginExecutionFailure)
    source_stmt = ins_stmt_source.values(records)
    
    source_stmt = source_stmt.on_conflict_do_update(
        constraint=PluginExecutionFailure.__table__.primary_key,
        set_={"failure_reason": source_stmt.excluded.failure_reason,
              "failure_detail": source_stmt.excluded.failure_detail,
              "request": source_stmt.excluded.request}
    ).returning(
        PluginExecutionFailure.id,
        PluginExecutionFailure.timestamp,
        PluginExecutionFailure.plugin_id,
        PluginExecutionFailure.failure_reason,
        PluginExecutionFailure.failure_detail,
        PluginExecutionFailure.request
    )
    result = await db.execute(source_stmt)
    records = result.fetchall()
    await db.commit()
    return records 
