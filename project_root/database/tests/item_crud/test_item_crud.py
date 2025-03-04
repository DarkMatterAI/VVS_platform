import pytest

@pytest.mark.asyncio
async def test_item_create(create_item):
    item = 'item_create_test_1'
    item_record = await create_item(item)
    assert item_record.item == item

@pytest.mark.asyncio
async def test_item_get(create_item, get_item):
    item_record = await get_item(999999)
    assert item_record is None 

    item = await create_item()
    item_record = await get_item(item.id)
    assert item_record is not None

@pytest.mark.asyncio
async def test_item_delete(create_item, get_item, delete_item):
    item = await create_item()

    await delete_item(item)
    item_record = await get_item(item.id)
    assert item_record is None 

@pytest.mark.asyncio
async def test_get_items(create_item, get_items):
    items = []
    for i in range(5):
        item_record = await create_item()
        items.append(item_record)

    item_ids = [item.id for item in items]
    records = await get_items(item_ids)
    assert len(records) == len(items)
    for record in records:
        assert record.id in item_ids 
