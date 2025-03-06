import pytest

from vvs_database import crud 

@pytest.mark.asyncio
async def test_item_create(create_item):
    item = 'item_create_test_1'
    item_record = await create_item(item)
    assert item_record.item == item

@pytest.mark.asyncio
async def test_item_get(db_session, create_item):
    item_record = await crud.get_item(db_session, 999999)
    assert item_record is None 

    item = await create_item()
    item_record = await crud.get_item(db_session, item.id)
    assert item_record is not None

@pytest.mark.asyncio
async def test_item_delete(db_session, create_item):
    item = await create_item()

    _ = await crud.delete_item(db_session, item)

    item_record = await crud.get_item(db_session, item.id)
    assert item_record is None 

@pytest.mark.asyncio
async def test_get_items(db_session, create_item):
    items = []
    for i in range(5):
        item_record = await create_item()
        items.append(item_record)

    item_ids = [item.id for item in items]
    records = await crud.get_items(db_session, item_ids)
    assert len(records) == len(items)
    for record in records:
        assert record.id in item_ids 
