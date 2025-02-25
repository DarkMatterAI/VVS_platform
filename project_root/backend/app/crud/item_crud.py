from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert


from typing import Optional, Union, List, Dict, Tuple  

from app import models

from pydantic import BaseModel 

class NewItem(BaseModel):
    external_id: Optional[Union[int, str]]
    item: str

class NewScore(BaseModel):
    item_id: int 
    score: float 

async def create_item(db: AsyncSession, item: str) -> models.Item:
    item = models.Item(item=item)
    db.add(item)
    await db.commit()
    return item 

async def get_item(db: AsyncSession, item_id: int) -> Optional[models.Item]:
    result = await db.execute(select(models.Item).filter(models.Item.id == item_id))
    return result.scalar_one_or_none()

async def delete_item(db: AsyncSession, item: models.Item) -> models.Item:    
    await db.delete(item)
    await db.commit()
    return item 


async def create_item_source(db: AsyncSession, 
                             item_id: int, 
                             plugin_id: int, 
                             external_id: Optional[str]=None
                             ) -> models.ItemSource:
    item_source = models.ItemSource(item_id=item_id, plugin_id=plugin_id, external_id=external_id)
    db.add(item_source)
    await db.commit()
    return item_source  

async def get_item_source(db: AsyncSession, item_id: int, plugin_id: int
                          ) -> Optional[models.ItemSource]:
    result = await db.execute(
        select(models.ItemSource)
        .filter(
            models.ItemSource.item_id == item_id,
            models.ItemSource.plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def delete_item_source(db: AsyncSession, item_source: models.ItemSource) -> bool:    
    await db.delete(item_source)
    await db.commit()
    return item_source 


async def create_item_score(db: AsyncSession, 
                             item_id: int, 
                             plugin_id: int, 
                             score: float
                             ) -> models.ItemScore:
    item_score = models.ItemScore(item_id=item_id, plugin_id=plugin_id, score=score)
    db.add(item_score)
    await db.commit()
    return item_score

async def get_item_score(db: AsyncSession, item_id: int, plugin_id: int
                          ) -> Optional[models.ItemSource]:
    result = await db.execute(
        select(models.ItemScore)
        .filter(
            models.ItemScore.item_id == item_id,
            models.ItemScore.plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def delete_item_score(db: AsyncSession, item_score: models.ItemScore) -> models.ItemScore:
    await db.delete(item_score)
    await db.commit()
    return item_score 

async def cleanup_unreferenced_items(db: AsyncSession) -> int:
    return await models.Item.cleanup_unreferenced(db)


async def item_checkin(db: AsyncSession, new_items: List[NewItem], plugin_id: int) -> Dict:
    # Create a list of unique items
    unique_items = {ni.item: ni.external_id for ni in new_items}

    # Create the insert instance for the Item table.
    ins_stmt = pg_insert(models.Item)
    item_stmt = ins_stmt.values([{"item": item} for item in unique_items.keys()])
    item_stmt = item_stmt.on_conflict_do_update(
        index_elements=["item"],
        set_={"item": item_stmt.excluded.item}  # Note: using the instance's excluded attribute
    ).returning(models.Item.id, models.Item.item, models.Item.created_at)

    result = await db.execute(item_stmt)
    item_records = result.fetchall()

    # Prepare data for ItemSource using the returned item IDs
    item_source_data = [
        {
            "item_id": item.id,
            "external_id": unique_items[item.item],
            "plugin_id": plugin_id,
        }
        for item in item_records
    ]
    
    # Similarly, create the insert instance for the ItemSource table.
    ins_stmt_source = pg_insert(models.ItemSource)
    source_stmt = ins_stmt_source.values(item_source_data)
    source_stmt = source_stmt.on_conflict_do_update(
        index_elements=["item_id", "plugin_id"],
        set_={"external_id": source_stmt.excluded.external_id}
    ).returning(
        models.ItemSource.item_id,
        models.ItemSource.external_id,
        models.ItemSource.plugin_id,
        models.ItemSource.created_at
    )

    result = await db.execute(source_stmt)
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


async def score_checkin(db: AsyncSession, 
                        new_scores: List[NewScore], 
                        plugin_id: int) -> List[models.ItemScore]:
    
    unique_scores = {ns.item_id: ns for ns in new_scores}

    values = [
        {
            "item_id": score.item_id,
            "plugin_id": plugin_id,
            "score": score.score,
        }
        for score in unique_scores.values()
    ]
    
    stmt = pg_insert(models.ItemScore).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[models.ItemScore.item_id, models.ItemScore.plugin_id],
        set_={
            'score': stmt.excluded.score,
            'created_at': func.now()
        }
    ).returning(
        models.ItemScore.item_id,
        models.ItemScore.plugin_id,
        models.ItemScore.score,
        models.ItemScore.created_at
    )
    
    result = await db.execute(stmt)
    score_records = result.fetchall()
    await db.commit()

    result_dict = {i.item_id:i for i in score_records}
    result = [result_dict[ns.item_id] for ns in new_scores]

    return result 





































# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy.future import select
# from sqlalchemy import select, union_all, exists, not_, column, text, func
# from sqlalchemy.dialects.postgresql import insert as pg_insert
# from sqlalchemy.sql import values
# from fastapi import HTTPException

# from typing import Optional, List, Dict, Tuple

# from app import models, schemas 


# async def get_item(db: AsyncSession, item_id: int) -> Optional[models.Item]:
#     result = await db.execute(select(models.Item).filter(models.Item.id == item_id))
#     return result.scalar_one_or_none()

# async def get_item_source(db: AsyncSession, item_id: int, plugin_id: int
#                           ) -> Optional[models.ItemSource]:
#     result = await db.execute(
#         select(models.ItemSource)
#         .filter(
#             models.ItemSource.item_id == item_id,
#             models.ItemSource.plugin_id == plugin_id
#         )
#     )
#     return result.scalar_one_or_none()

# async def delete_item(db: AsyncSession, item_id: int) -> bool:
#     item = await get_item(db, item_id)
#     if item is None:
#         return item 
    
#     await db.delete(item)
#     await db.commit()
#     return item 

# async def delete_item_source(db: AsyncSession, item_id: int, plugin_id: int) -> bool:
#     source = await get_item_source(db, item_id, plugin_id)
#     if source is None:
#         return source 
    
#     await db.delete(source)
#     await db.commit()
#     return source 

# async def cleanup_unreferenced_items(db: AsyncSession) -> int:
#     return await models.Item.cleanup_unreferenced(db)


# async def create_values_cte_async(db: AsyncSession, columns: List[str], data: List[Tuple], alias_name: str):
#     """Create a Common Table Expression (CTE) from values asynchronously."""
#     v = values(*[column(col) for col in columns]).data(data).alias("v")
#     cte = select(*[v.c[col] for col in columns]).cte(alias_name)
#     return cte

# async def build_upsert_query_async(
#     table, 
#     columns: List[str], 
#     source_cte, 
#     conflict_columns: List[str], 
#     return_columns: List[str]
# ):
#     """Build an upsert query with returning clause asynchronously."""
#     insert_stmt = (
#         pg_insert(table)
#         .from_select(columns, select(*[source_cte.c[col] for col in columns]))
#         .on_conflict_do_nothing(index_elements=conflict_columns)
#         .returning(*[getattr(table, col) for col in return_columns])
#     )
#     return insert_stmt.cte(f"inserted_or_existing_{table.__tablename__}")

# async def get_existing_records_query_async(
#     table, 
#     inserted_cte, 
#     join_conditions, 
#     return_columns: List[str]
# ):
#     """Build a query to get existing records that weren't inserted asynchronously."""
#     return (
#         select(*[getattr(table, col) for col in return_columns])
#         .join(*join_conditions)
#         .where(not_(exists(
#             select(1)
#             .select_from(inserted_cte)
#             .where(*[
#                 getattr(inserted_cte.c, col) == getattr(table, col)
#                 for col in return_columns
#             ])
#         )))
#     )

# async def item_checkin(
#     db: AsyncSession,
#     new_items: List[schemas.NewItem],
#     plugin_id: int
# ) -> Dict:
#     """Process new items and their sources asynchronously."""
    
#     # Create mapping of unique items to external_ids
#     unique_items = {ni.item: ni.external_id for ni in new_items}

#     # --- Handle Item table operations ---
#     items_cte = await create_values_cte_async(
#         db=db,
#         columns=["item"],
#         data=[(item,) for item in unique_items.keys()],
#         alias_name="items_to_insert"
#     )

#     inserted_items_cte = await build_upsert_query_async(
#         table=models.Item,
#         columns=["item"],
#         source_cte=items_cte,
#         conflict_columns=["item"],
#         return_columns=["id", "item", "created_at"]  # Added created_at
#     )

#     # Get both inserted and existing items
#     q_inserted_items = select(
#         inserted_items_cte.c.id, 
#         inserted_items_cte.c.item,
#         inserted_items_cte.c.created_at  # Added created_at
#     )
#     q_existing_items = await get_existing_records_query_async(
#         table=models.Item,
#         inserted_cte=inserted_items_cte,
#         join_conditions=(items_cte, models.Item.item == items_cte.c.item),
#         return_columns=["id", "item", "created_at"]  # Added created_at
#     )
    
#     result = await db.execute(q_inserted_items.union_all(q_existing_items))
#     item_records = result.fetchall()

#     # --- Handle ItemSource table operations ---
#     item_source_data = [
#         (row.id, unique_items[row.item], plugin_id)
#         for row in item_records
#     ]

#     sources_cte = await create_values_cte_async(
#         db=db,
#         columns=["item_id", "external_id", "plugin_id"],
#         data=item_source_data,
#         alias_name="sources_to_insert"
#     )

#     inserted_sources_cte = await build_upsert_query_async(
#         table=models.ItemSource,
#         columns=["item_id", "external_id", "plugin_id"],
#         source_cte=sources_cte,
#         conflict_columns=["item_id", "plugin_id"],
#         return_columns=["item_id", "external_id", "plugin_id", "created_at"]  # Added created_at
#     )

#     # Get both inserted and existing sources
#     q_inserted_sources = select(
#         inserted_sources_cte.c.item_id,
#         inserted_sources_cte.c.external_id,
#         inserted_sources_cte.c.plugin_id,
#         inserted_sources_cte.c.created_at  # Added created_at
#     )
#     q_existing_sources = await get_existing_records_query_async(
#         table=models.ItemSource,
#         inserted_cte=inserted_sources_cte,
#         join_conditions=(
#             sources_cte,
#             models.ItemSource.item_id == sources_cte.c.item_id
#         ),
#         return_columns=["item_id", "external_id", "plugin_id", "created_at"]  # Added created_at
#     )

#     result = await db.execute(q_inserted_sources.union_all(q_existing_sources))
#     item_source_records = result.fetchall()

#     await db.commit()

#     item_records_dict = {i.item : i for i in item_records}
#     item_source_records_dict = {i.item_id:i for i in item_source_records}

#     item_records_response = [item_records_dict[ni.item] for ni in new_items]
#     item_source_records_response = [item_source_records_dict[i.id] for i in item_records_response]
    
#     return {
#         "items": item_records_response,
#         "item_sources": item_source_records_response
#     }


# async def get_item_score(db: AsyncSession, item_id: int, plugin_id: int
#                           ) -> Optional[models.ItemSource]:
#     result = await db.execute(
#         select(models.ItemScore)
#         .filter(
#             models.ItemScore.item_id == item_id,
#             models.ItemScore.plugin_id == plugin_id
#         )
#     )
#     return result.scalar_one_or_none()

# async def delete_item_score(db: AsyncSession, item_id: int, plugin_id: int) -> bool:
#     score = await get_item_score(db, item_id, plugin_id)
#     if score is None:
#         return score 
    
#     await db.delete(score)
#     await db.commit()
#     return score 

# async def score_checkin(db: AsyncSession, 
#                         new_scores: List[schemas.NewScore], 
#                         plugin_id: int) -> List[schemas.ItemScore]:
    
#     unique_scores = {ns.item_id: ns for ns in new_scores}
#     item_ids = list(unique_scores.keys())

#     # Check if all item_ids exist in the Items table
#     stmt = select(models.Item.id).where(models.Item.id.in_(item_ids))
#     result = await db.execute(stmt)
#     existing_item_ids = set(result.scalars().all())
    
#     # Find missing item_ids
#     missing_item_ids = set(item_ids) - existing_item_ids
#     if missing_item_ids:
#         raise HTTPException(status_code=404, 
#                             detail=f"The following item_ids do not exist: {missing_item_ids}")

#     values = [
#         {
#             "item_id": score.item_id,
#             "plugin_id": plugin_id,
#             "score": score.score,
#         }
#         for score in unique_scores.values()
#     ]
    
#     stmt = pg_insert(models.ItemScore).values(values)
#     stmt = stmt.on_conflict_do_update(
#         index_elements=[models.ItemScore.item_id, models.ItemScore.plugin_id],
#         set_={
#             'score': stmt.excluded.score,
#             'created_at': func.now()
#         }
#     ).returning(
#         models.ItemScore.item_id,
#         models.ItemScore.plugin_id,
#         models.ItemScore.score,
#         models.ItemScore.created_at
#     )
    
#     result = await db.execute(stmt)
#     rows = result.all()
    
#     result = [
#                 schemas.ItemScore(
#                     item_id=row.item_id,
#                     plugin_id=row.plugin_id,
#                     score=row.score,
#                     created_at=row.created_at
#                 )
#                 for row in rows
#             ]
#     result_dict = {i.item_id:i for i in result}
#     result = [result_dict[ns.item_id] for ns in new_scores]
#     return result 
