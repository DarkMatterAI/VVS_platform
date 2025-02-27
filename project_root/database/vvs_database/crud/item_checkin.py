from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import List, Dict

from vvs_database.models import Item, ItemSource, ItemScore
from vvs_database import schemas 

async def item_checkin(db: AsyncSession, new_items: List[schemas.NewItem], plugin_id: int) -> Dict:
    """Check in multiple items, creating or updating them as needed."""
    # Create a list of unique items
    unique_items = {ni.item: ni.external_id for ni in new_items}

    # Create the insert instance for the Item table
    ins_stmt = pg_insert(Item)
    item_stmt = ins_stmt.values([{"item": item} for item in unique_items.keys()])
    item_stmt = item_stmt.on_conflict_do_update(
        index_elements=["item"],
        set_={"item": item_stmt.excluded.item}
    ).returning(Item.id, Item.item, Item.created_at)

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
    
    # Similarly, create the insert instance for the ItemSource table
    ins_stmt_source = pg_insert(ItemSource)
    source_stmt = ins_stmt_source.values(item_source_data)
    source_stmt = source_stmt.on_conflict_do_update(
        index_elements=["item_id", "plugin_id"],
        set_={"external_id": source_stmt.excluded.external_id}
    ).returning(
        ItemSource.item_id,
        ItemSource.external_id,
        ItemSource.plugin_id,
        ItemSource.created_at
    )

    result = await db.execute(source_stmt)
    item_source_records = result.fetchall()

    await db.commit()

    item_records_dict = {i.item: i for i in item_records}
    item_source_records_dict = {i.item_id: i for i in item_source_records}

    item_records_response = [item_records_dict[ni.item] for ni in new_items]
    item_source_records_response = [item_source_records_dict[i.id] for i in item_records_response]
    
    return {
        "items": item_records_response,
        "item_sources": item_source_records_response
    }

async def score_checkin(
    db: AsyncSession, 
    new_scores: List[schemas.NewScore], 
    plugin_id: int
) -> List[ItemScore]:
    """Check in multiple scores, creating or updating them as needed."""
    unique_scores = {ns.item_id: ns for ns in new_scores}

    values = [
        {
            "item_id": score.item_id,
            "plugin_id": plugin_id,
            "score": score.score,
        }
        for score in unique_scores.values()
    ]
    
    stmt = pg_insert(ItemScore).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[ItemScore.item_id, ItemScore.plugin_id],
        set_={
            'score': stmt.excluded.score,
            'created_at': func.now()
        }
    ).returning(
        ItemScore.item_id,
        ItemScore.plugin_id,
        ItemScore.score,
        ItemScore.created_at
    )
    
    result = await db.execute(stmt)
    score_records = result.fetchall()
    await db.commit()

    result_dict = {i.item_id: i for i in score_records}
    result = [result_dict[ns.item_id] for ns in new_scores]

    return result

# async def data_source_response_checkin(db: AsyncSession, 
#                                        response: schemas.DataSourceResponse,
#                                        plugin_id: int):
#     if (not response.valid) or (not response.result):
#         return []
    
#     results = response = response.model_dump()['result']
    
#     new_items = [schemas.NewItem(**{'external_id' : r['external_id'], 
#                                     'item' : r['item']}) 
#                  for r in results]
#     new_items = await item_checkin(db, new_items, plugin_id)
#     new_items = new_items['items']

#     response = response.model_dump()['result']
#     for (i, r) in zip(new_items, response):
#         r['item_id'] = i.id 

#     return response
    
