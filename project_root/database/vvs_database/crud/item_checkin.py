from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import select
from sqlalchemy.orm import selectinload
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
from vvs_database.utils import chunked, with_deadlock_retry

# async def upsert_items(db: AsyncSession, new_items: List[dict]) -> List:
#     ins_stmt = pg_insert(Item)
#     item_stmt = ins_stmt.values(new_items)
#     item_stmt = item_stmt.on_conflict_do_update(
#         index_elements=["item"],
#         set_={"item": item_stmt.excluded.item}
#     ).returning(Item.id, Item.item, Item.created_at)

#     result = await db.execute(item_stmt)
#     item_records = result.fetchall()
#     return item_records 

# async def upsert_items(db: AsyncSession, new_items: list[dict]) -> list[Item]:
#     """
#     Insert every str in *new_items* into ITEMS if it doesn't exist yet and
#     return **one Item row per element of *new_items*** in the original order
#     (duplicates included).
#     """
#     if not new_items:
#         return []

#     # ------------------------------------------------------------------
#     # 1) insert‑ignore the *unique* strings
#     # ------------------------------------------------------------------
#     uniq_values = {d["item"] for d in new_items}
#     ins_stmt = (
#         pg_insert(Item)
#         .values([{"item": s} for s in uniq_values])
#         .on_conflict_do_nothing(index_elements=["item"])          # ← no UPDATE
#         .returning(Item.id, Item.item, Item.created_at)
#     )
#     inserted = (await db.execute(ins_stmt)).fetchall()
#     inserted_map = {row.item: row for row in inserted}

#     # ------------------------------------------------------------------
#     # 2) fetch any rows that already existed
#     # ------------------------------------------------------------------
#     missing = [s for s in uniq_values if s not in inserted_map]
#     if missing:
#         rows = (
#             await db.execute(
#                 select(Item.id, Item.item, Item.created_at).where(Item.item.in_(missing))
#             )
#         ).fetchall()
#         inserted_map.update({row.item: row for row in rows})

#     # ------------------------------------------------------------------
#     # 3) rebuild the list in the exact input order (duplicates kept)
#     # ------------------------------------------------------------------
#     return [inserted_map[d["item"]] for d in new_items]

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
        batch_rows = await with_deadlock_retry(
            db, lambda: _upsert_items_single_batch(db, unique_payload)
        )
        # map back to original order (duplicates kept)
        row_map = {r.item: r for r in batch_rows}
        results.extend(row_map[s] for s in batch_strings)

    return results

# async def upsert_item_sources(db: AsyncSession, new_item_sources: List[dict]) -> List: 
#     ins_stmt_source = pg_insert(ItemSource)
#     source_stmt = ins_stmt_source.values(new_item_sources)
#     source_stmt = source_stmt.on_conflict_do_update(
#         index_elements=["item_id", "plugin_id"],
#         set_={"external_id": source_stmt.excluded.external_id}
#     ).returning(
#         ItemSource.item_id,
#         ItemSource.external_id,
#         ItemSource.plugin_id,
#         ItemSource.created_at
#     )

#     result = await db.execute(source_stmt)
#     item_source_records = result.fetchall()
#     return item_source_records

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
        rows = await with_deadlock_retry(db, lambda: _upsert_item_sources_single(db, chunk))
        out.extend(rows)
    return out

# async def item_checkin(db: AsyncSession, new_items: List[schemas.NewItem], plugin_id: Optional[int]) -> Dict:
#     """Check in multiple items, creating or updating them as needed."""
#     # Create a list of unique items
#     unique_items = {ni.item: ni.external_id for ni in new_items}

#     # Create the insert instance for the Item table
#     item_dicts = [{"item": item} for item in unique_items.keys()]
#     item_records = await upsert_items(db, item_dicts)

#     item_records_dict = {i.item: i for i in item_records}
#     item_records_response = [item_records_dict[ni.item] for ni in new_items]

#     item_source_records_dict = {}
#     item_source_records_response = []

#     # Prepare data for ItemSource using the returned item IDs
#     if plugin_id is not None:
#         item_source_data = []
#         for item in item_records:
#             external_id = unique_items[item.item]
#             external_id = external_id if external_id is None else str(external_id)
#             item_source_data.append({
#                 "item_id": item.id,
#                 "external_id": external_id,
#                 "plugin_id": plugin_id,
#             })

#         item_source_records = await upsert_item_sources(db, item_source_data)

#         await db.commit()

#         item_source_records_dict = {i.item_id: i for i in item_source_records}
#         item_source_records_response = [item_source_records_dict[i.id] for i in item_records_response]
    
#     return {
#         "items": item_records_response,
#         "item_sources": item_source_records_response
#     }

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

# async def result_checkin(
#     db: AsyncSession, 
#     new_results: List[schemas.NewResult], 
#     plugin_id: int
# ) -> List[ItemResult]:
#     """Check in multiple results, creating or updating them as needed."""
#     unique_results = {nr.item_id: nr for nr in new_results}

#     values = [
#         {
#             "item_id": result.item_id,
#             "plugin_id": plugin_id,
#             "valid": result.valid,
#             "score": result.score,
#             "embedding": result.embedding,
#         }
#         for result in unique_results.values()
#     ]
    
#     stmt = pg_insert(ItemResult).values(values)
#     stmt = stmt.on_conflict_do_update(
#         index_elements=[ItemResult.item_id, ItemResult.plugin_id],
#         set_={
#             'valid': stmt.excluded.valid,
#             'score': stmt.excluded.score,
#             'embedding': stmt.excluded.embedding,
#             'created_at': func.now()
#         }
#     ).returning(
#         ItemResult.item_id,
#         ItemResult.plugin_id,
#         ItemResult.valid,
#         ItemResult.score,
#         ItemResult.embedding,
#         ItemResult.created_at
#     )
    
#     result = await db.execute(stmt)
#     result_records = result.fetchall()
#     await db.commit()

#     result_dict = {i.item_id: i for i in result_records}
#     result = [result_dict[nr.item_id] for nr in new_results]

#     return result

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
        rows = await with_deadlock_retry(
            db, lambda: _upsert_item_results_single(db, values, plugin_id)
        )
        row_map = {r.item_id: r for r in rows}

        # 3) copy back to *out* preserving duplicates & original order
        for pos, rec in batch:
            out[pos] = row_map[rec.item_id]

    await db.commit()
    return out      # list[ItemResult] in same order/length as input

# async def assembly_checkin(db: AsyncSession, 
#                           new_assemblies: List[schemas.NewAssembly], 
#                           plugin_id: int) -> Dict:
#     """
#     Check in multiple assemblies, creating or updating them as needed.
    
#     Optimized implementation with bulk operations for assembly creation.
#     """
#     # Step 1: Check in the assembly result items (already optimized)
#     item_checkin_result = await item_checkin(db, new_assemblies, plugin_id)
#     checked_in_items = item_checkin_result["items"]
    
#     # Create a mapping from item string to item ID for easy lookup
#     item_map = {item.item: item.id for item in checked_in_items}


#     unique_assemblies = {}
#     assembly_to_components = {}
#     assembly_keys = []
    
#     for i, new_assembly in enumerate(new_assemblies):
#         product_id = item_map[new_assembly.item]
        
#         # Sort components by assembly index
#         sorted_components = sorted(new_assembly.components, key=lambda x: x.assembly_index)
#         component_ids = [comp.item_id for comp in sorted_components]
        
#         # Generate assembly key
#         assembly_key = f"{plugin_id}_{'_'.join(map(str, component_ids))}_{product_id}"

#         # Generate component key
#         component_key = f"{plugin_id}_{'_'.join(map(str, component_ids))}"
        
#         # Store for reconstruction
#         assembly_keys.append(assembly_key)

#         # Store data for bulk creation
#         unique_assemblies[assembly_key] = {
#             "product_id": product_id,
#             "plugin_id": plugin_id,
#             "assembly_key": assembly_key,
#             "component_key": component_key
#         }
        
#         # Store component data with placeholder for assembly_id
#         assembly_to_components[assembly_key] = [
#             {
#                 "assembly_index": comp.assembly_index,
#                 "component_id": comp.item_id
#             }
#             for comp in new_assembly.components
#         ]
    
#     # Step 3: Find existing assemblies to avoid duplicates
#     unique_assembly_keys = list(unique_assemblies.keys())
#     result = await db.execute(
#         select(Assembly)
#         .where(Assembly.assembly_key.in_(unique_assembly_keys))
#     )
#     existing_assemblies = result.scalars().all()
#     existing_keys = {a.assembly_key: a for a in existing_assemblies}
    
#     # Step 4: Insert new assemblies (those not in existing_keys)
#     new_assembly_data = []
#     new_assembly_keys = []
#     for key, data in unique_assemblies.items():
#         if key not in existing_keys:
#             new_assembly_data.append(data)
#             new_assembly_keys.append(key)
    
#     inserted_assemblies = []
#     if new_assembly_data:
#         stmt = pg_insert(Assembly).values(new_assembly_data)
#         stmt = stmt.returning(Assembly.assembly_id, Assembly.assembly_key)
#         result = await db.execute(stmt)
#         inserted_assemblies = result.fetchall()
        
#     # Create mapping of all assembly keys to assembly IDs
#     all_assembly_mapping = {**{a.assembly_key: a.assembly_id for a in existing_assemblies}}
#     for assembly in inserted_assemblies:
#         all_assembly_mapping[assembly.assembly_key] = assembly.assembly_id
    
#     # Step 5: Insert components for new assemblies
#     for assembly_key in new_assembly_keys:
#         assembly_id = all_assembly_mapping[assembly_key]
#         components = assembly_to_components[assembly_key]
        
#         component_insert_data = [
#             {
#                 "assembly_id": assembly_id,
#                 "assembly_index": comp["assembly_index"],
#                 "component_id": comp["component_id"]
#             }
#             for comp in components
#         ]
        
#         if component_insert_data:
#             await db.execute(
#                 pg_insert(AssemblyComponent).values(component_insert_data)
#             )
    
#     # Step 6: Load all assemblies with their components for the response
#     result = await db.execute(
#         select(Assembly)
#         .options(selectinload(Assembly.components))
#         .where(Assembly.assembly_key.in_(assembly_keys))
#     )
#     all_assemblies = result.scalars().all()
    
#     # Map to original order for the response
#     assembly_mapping = {a.assembly_key: a for a in all_assemblies}
#     assemblies_result = [assembly_mapping[key] for key in assembly_keys]
    
#     await db.commit()
    
#     return {
#         "items": checked_in_items,
#         "item_sources": item_checkin_result["item_sources"],
#         "assemblies": assemblies_result
#     }

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
        rows = await with_deadlock_retry(db, lambda: _insert_assemblies_single(db, chunk))
        asm_id_map.update({k: i for i, k in rows})

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
        await with_deadlock_retry(db, lambda: _insert_components_single(db, chunk))

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
