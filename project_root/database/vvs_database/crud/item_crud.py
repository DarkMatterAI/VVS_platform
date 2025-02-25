from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Optional, List, Dict

from vvs_database.models import Item, ItemSource, ItemScore
from vvs_database.schemas import NewItem, NewScore

async def create_item(db: AsyncSession, item: str) -> Item:
    """Create a new item."""
    item = Item(item=item)
    db.add(item)
    await db.commit()
    return item 

async def get_item(db: AsyncSession, item_id: int) -> Optional[Item]:
    """Get an item by ID."""
    result = await db.execute(select(Item).filter(Item.id == item_id))
    return result.scalar_one_or_none()

async def delete_item(db: AsyncSession, item: Item) -> Item:
    """Delete an item."""
    await db.delete(item)
    await db.commit()
    return item 

async def create_item_source(
    db: AsyncSession,
    item_id: int, 
    plugin_id: int, 
    external_id: Optional[str]=None
) -> ItemSource:
    """Create a new item source."""
    item_source = ItemSource(item_id=item_id, plugin_id=plugin_id, external_id=external_id)
    db.add(item_source)
    await db.commit()
    return item_source  

async def get_item_source(
    db: AsyncSession, 
    item_id: int, 
    plugin_id: int
) -> Optional[ItemSource]:
    """Get an item source by item ID and plugin ID."""
    result = await db.execute(
        select(ItemSource)
        .filter(
            ItemSource.item_id == item_id,
            ItemSource.plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def delete_item_source(db: AsyncSession, item_source: ItemSource) -> ItemSource:
    """Delete an item source."""
    await db.delete(item_source)
    await db.commit()
    return item_source 

async def create_item_score(
    db: AsyncSession,
    item_id: int, 
    plugin_id: int, 
    score: float
) -> ItemScore:
    """Create a new item score."""
    item_score = ItemScore(item_id=item_id, plugin_id=plugin_id, score=score)
    db.add(item_score)
    await db.commit()
    return item_score

async def get_item_score(
    db: AsyncSession, 
    item_id: int, 
    plugin_id: int
) -> Optional[ItemScore]:
    """Get an item score by item ID and plugin ID."""
    result = await db.execute(
        select(ItemScore)
        .filter(
            ItemScore.item_id == item_id,
            ItemScore.plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def delete_item_score(db: AsyncSession, item_score: ItemScore) -> ItemScore:
    """Delete an item score."""
    await db.delete(item_score)
    await db.commit()
    return item_score 

async def cleanup_unreferenced_items(db: AsyncSession) -> int:
    """Delete items that aren't referenced in other tables."""
    return await Item.cleanup_unreferenced(db)

async def item_checkin(db: AsyncSession, new_items: List[NewItem], plugin_id: int) -> Dict:
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
    new_scores: List[NewScore], 
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