import pytest

plugin_api_str = '/api/v1/plugins'


@pytest.mark.asyncio
async def test_item_create(client, create_item):
    item = 'item_create_test_1'
    item_record = await create_item(item)
    assert item_record.item == item

@pytest.mark.asyncio
async def test_item_get(client, create_item, get_item):
    item_record = await get_item(999999)
    assert item_record is None 

    item = await create_item()
    item_record = await get_item(item.id)
    assert item_record is not None

@pytest.mark.asyncio
async def test_item_delete(client, create_item, get_item, delete_item):
    item = await create_item()

    await delete_item(item)
    item_record = await get_item(item.id)
    assert item_record is None 


