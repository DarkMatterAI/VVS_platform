from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert


from typing import Optional, Union, List, Dict, Tuple  

from vvs_database import models 

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



