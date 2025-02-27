from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List

from vvs_database.models import Item, ItemSource, ItemResult

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

async def get_item_by_name(db: AsyncSession, item: str) -> Optional[Item]:
    """Get an item by ID."""
    result = await db.execute(select(Item).filter(Item.item == item))
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

async def create_item_result(
    db: AsyncSession,
    item_id: int, 
    plugin_id: int, 
    valid: bool,
    score: Optional[float] = None,
    embedding: Optional[List[float]] = None
) -> ItemResult:
    """Create a new item result."""
    item_result = ItemResult(
        item_id=item_id, 
        plugin_id=plugin_id, 
        valid=valid,
        score=score,
        embedding=embedding
    )
    db.add(item_result)
    await db.commit()
    return item_result

async def get_item_result(
    db: AsyncSession, 
    item_id: int, 
    plugin_id: int
) -> Optional[ItemResult]:
    """Get an item result by item ID and plugin ID."""
    result = await db.execute(
        select(ItemResult)
        .filter(
            ItemResult.item_id == item_id,
            ItemResult.plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def delete_item_result(db: AsyncSession, item_result: ItemResult) -> ItemResult:
    """Delete an item result."""
    await db.delete(item_result)
    await db.commit()
    return item_result

async def cleanup_unreferenced_items(db: AsyncSession) -> int:
    """Delete items that aren't referenced in other tables."""
    return await Item.cleanup_unreferenced(db)

