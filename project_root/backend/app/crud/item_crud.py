from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import select, union_all, exists, not_, column, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import values

from typing import Optional, List, Dict, Tuple

from app import models, schemas 


async def get_item(db: AsyncSession, item_id: int) -> Optional[models.Item]:
    result = await db.execute(select(models.Item).filter(models.Item.id == item_id))
    return result.scalar_one_or_none()

async def get_item_source(db: AsyncSession, item_id: int, plugin_id: int
                          ) -> Optional[models.ItemSource]:
    result = await db.execute(
        select(models.ItemSource)
        .filter(
            models.ItemSource.item_id == item_id,
            models.ItemSource.source_plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def delete_item(db: AsyncSession, item_id: int) -> bool:
    item = await get_item(db, item_id)
    if item is None:
        return item 
    
    await db.delete(item)
    await db.commit()
    return item 

async def delete_item_source(db: AsyncSession, item_id: int, plugin_id: int) -> bool:
    source = await get_item_source(db, item_id, plugin_id)
    if source is None:
        return source 
    
    await db.delete(source)
    await db.commit()
    return source 

async def cleanup_unreferenced_items(db: AsyncSession) -> int:
    return await models.Item.cleanup_unreferenced(db)


async def create_values_cte_async(db: AsyncSession, columns: List[str], data: List[Tuple], alias_name: str):
    """Create a Common Table Expression (CTE) from values asynchronously."""
    v = values(*[column(col) for col in columns]).data(data).alias("v")
    cte = select(*[v.c[col] for col in columns]).cte(alias_name)
    return cte

async def build_upsert_query_async(
    table, 
    columns: List[str], 
    source_cte, 
    conflict_columns: List[str], 
    return_columns: List[str]
):
    """Build an upsert query with returning clause asynchronously."""
    insert_stmt = (
        pg_insert(table)
        .from_select(columns, select(*[source_cte.c[col] for col in columns]))
        .on_conflict_do_nothing(index_elements=conflict_columns)
        .returning(*[getattr(table, col) for col in return_columns])
    )
    return insert_stmt.cte(f"inserted_or_existing_{table.__tablename__}")

async def get_existing_records_query_async(
    table, 
    inserted_cte, 
    join_conditions, 
    return_columns: List[str]
):
    """Build a query to get existing records that weren't inserted asynchronously."""
    return (
        select(*[getattr(table, col) for col in return_columns])
        .join(*join_conditions)
        .where(not_(exists(
            select(1)
            .select_from(inserted_cte)
            .where(*[
                getattr(inserted_cte.c, col) == getattr(table, col)
                for col in return_columns
            ])
        )))
    )

async def item_checkin(
    db: AsyncSession,
    new_items: List[schemas.NewItem],
    plugin_id: int
) -> Dict:
    """Process new items and their sources asynchronously."""
    
    # Create mapping of unique items to external_ids
    unique_items = {ni.item: ni.external_id for ni in new_items}

    # --- Handle Item table operations ---
    items_cte = await create_values_cte_async(
        db=db,
        columns=["item"],
        data=[(item,) for item in unique_items.keys()],
        alias_name="items_to_insert"
    )

    inserted_items_cte = await build_upsert_query_async(
        table=models.Item,
        columns=["item"],
        source_cte=items_cte,
        conflict_columns=["item"],
        return_columns=["id", "item", "created_at"]  # Added created_at
    )

    # Get both inserted and existing items
    q_inserted_items = select(
        inserted_items_cte.c.id, 
        inserted_items_cte.c.item,
        inserted_items_cte.c.created_at  # Added created_at
    )
    q_existing_items = await get_existing_records_query_async(
        table=models.Item,
        inserted_cte=inserted_items_cte,
        join_conditions=(items_cte, models.Item.item == items_cte.c.item),
        return_columns=["id", "item", "created_at"]  # Added created_at
    )
    
    result = await db.execute(q_inserted_items.union_all(q_existing_items))
    item_records = result.fetchall()

    # --- Handle ItemSource table operations ---
    item_source_data = [
        (row.id, unique_items[row.item], plugin_id)
        for row in item_records
    ]

    sources_cte = await create_values_cte_async(
        db=db,
        columns=["item_id", "external_id", "source_plugin_id"],
        data=item_source_data,
        alias_name="sources_to_insert"
    )

    inserted_sources_cte = await build_upsert_query_async(
        table=models.ItemSource,
        columns=["item_id", "external_id", "source_plugin_id"],
        source_cte=sources_cte,
        conflict_columns=["item_id", "source_plugin_id"],
        return_columns=["item_id", "external_id", "source_plugin_id", "created_at"]  # Added created_at
    )

    # Get both inserted and existing sources
    q_inserted_sources = select(
        inserted_sources_cte.c.item_id,
        inserted_sources_cte.c.external_id,
        inserted_sources_cte.c.source_plugin_id,
        inserted_sources_cte.c.created_at  # Added created_at
    )
    q_existing_sources = await get_existing_records_query_async(
        table=models.ItemSource,
        inserted_cte=inserted_sources_cte,
        join_conditions=(
            sources_cte,
            models.ItemSource.item_id == sources_cte.c.item_id
        ),
        return_columns=["item_id", "external_id", "source_plugin_id", "created_at"]  # Added created_at
    )

    result = await db.execute(q_inserted_sources.union_all(q_existing_sources))
    item_source_records = result.fetchall()

    await db.commit()

    item_records_dict = {i.item : i for i in item_records}
    item_source_records_dict = {i.item_id:i for i in item_source_records}

    item_records_response = [item_records_dict[ni.item] for ni in new_items]
    item_source_records_response = [item_source_records_dict[i.id] for i in item_records_response]
    
    return {
        "items": item_records_response,
        "item_sources": item_source_records_response
    }

