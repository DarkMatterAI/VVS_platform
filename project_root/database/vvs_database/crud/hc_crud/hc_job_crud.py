from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func 
from typing import List, Optional

from vvs_database.models.job_models.hc_models import HCIterationJob, HCInputJob
from vvs_database.schemas import ItemData, InternalItem

async def latest_hc_iteration(db_session: AsyncSession,
                              hc_input_job_id: int
                             ) -> Optional[HCIterationJob]:
    stmt = (
        select(HCIterationJob)
        .where(HCIterationJob.input_id == hc_input_job_id)
        .order_by(HCIterationJob.iteration.desc())   # highest first
        .limit(1)
    )

    result = await db_session.execute(stmt)
    return result.scalars().first()

async def load_hc_input_job_items(db_session: AsyncSession,
                                  job: HCInputJob):
    input_items = {}
    await db_session.refresh(job, ["input_items"])
    for input_item in job.input_items:
        await db_session.refresh(input_item, ['item'])
        item_record = input_item.item 
        assembly_index = input_item.assembly_index
        item_data = ItemData(item_id=item_record.id,
                                external_id=input_item.external_id,
                                item=item_record.item)
        item_internal = InternalItem(item_data=item_data,
                                        valid=True,
                                        score=None,
                                        embeddings={},
                                        assembly_data=None,
                                        query_group=None)
        input_items[assembly_index] = item_internal
    return input_items 
            