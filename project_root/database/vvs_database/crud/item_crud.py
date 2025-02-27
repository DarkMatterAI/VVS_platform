from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List, Dict

from vvs_database.models import Item, ItemSource, ItemScore

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

