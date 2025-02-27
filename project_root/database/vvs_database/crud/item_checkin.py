from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import List, Dict

from vvs_database.models import Item, ItemSource, ItemResult
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

async def result_checkin(
    db: AsyncSession, 
    new_results: List[schemas.NewResult], 
    plugin_id: int
) -> List[ItemResult]:
    """Check in multiple results, creating or updating them as needed."""
    unique_results = {nr.item_id: nr for nr in new_results}

    values = [
        {
            "item_id": result.item_id,
            "plugin_id": plugin_id,
            "valid": result.valid,
            "score": result.score,
            "embedding": result.embedding,
        }
        for result in unique_results.values()
    ]
    
    stmt = pg_insert(ItemResult).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[ItemResult.item_id, ItemResult.plugin_id],
        set_={
            'valid': stmt.excluded.valid,
            'score': stmt.excluded.score,
            'embedding': stmt.excluded.embedding,
            'created_at': func.now()
        }
    ).returning(
        ItemResult.item_id,
        ItemResult.plugin_id,
        ItemResult.valid,
        ItemResult.score,
        ItemResult.embedding,
        ItemResult.created_at
    )
    
    result = await db.execute(stmt)
    result_records = result.fetchall()
    await db.commit()

    result_dict = {i.item_id: i for i in result_records}
    result = [result_dict[nr.item_id] for nr in new_results]

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
    
