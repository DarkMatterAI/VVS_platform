import pytest
import random 

@pytest.mark.asyncio
async def test_item_checkin(item_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

@pytest.mark.asyncio
async def test_item_checkin_duplicates(item_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": "bulk item 1", "external_id": "ext1"},
    ]

    results = await item_checkin(items_data, plugin.id)
    assert results['items'][0].item == results['items'][2].item
    assert results['items'][0].id == results['items'][2].id

@pytest.mark.asyncio
async def test_item_checkin_conflict(item_checkin, create_item_plugin_source):
    item, plugin, item_source = await create_item_plugin_source()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"},
        {"item": item.item, "external_id": item_source.external_id},
    ]

    results = await item_checkin(items_data, plugin.id)



@pytest.mark.asyncio
async def test_result_checkin(item_checkin, result_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

    result_data = [
        {
            'item_id': i.id, 
            'valid': True,
            'score': random.random(),
            'embedding': [random.random() for _ in range(3)]
        }
        for i in results['items']
    ]
    result_records = await result_checkin(result_data, plugin.id)

@pytest.mark.asyncio
async def test_result_checkin_no_score(item_checkin, result_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

    result_data = [
        {
            'item_id': i.id, 
            'valid': False,
            'embedding': [random.random() for _ in range(3)]
        }
        for i in results['items']
    ]
    result_records = await result_checkin(result_data, plugin.id)
    
    # Verify scores are None
    for record in result_records:
        assert record.score is None

@pytest.mark.asyncio
async def test_result_checkin_duplicates(item_checkin, result_checkin, create_test_embedding):
    plugin = await create_test_embedding()

    items_data = [
        {"item": "bulk item 1", "external_id": "ext1"},
        {"item": "bulk item 2", "external_id": "ext2"}
    ]

    results = await item_checkin(items_data, plugin.id)

    result_data = [
        {
            'item_id': i.id, 
            'valid': True,
            'score': random.random(),
            'embedding': [random.random() for _ in range(3)]
        }
        for i in results['items']
    ]
    result_data = list(result_data) + [result_data[0]]
    
    result_records = await result_checkin(result_data, plugin.id)

    assert result_records[0].item_id == result_records[2].item_id

@pytest.mark.asyncio
async def test_result_checkin_conflict(result_checkin, create_item,
                                     create_item_result, create_item_plugin_source):
    item1, plugin, item_source = await create_item_plugin_source()
    score = 9.42
    embedding1 = [0.1, 0.2, 0.3, 0.8]
    item_result = await create_item_result(
        item1.id, plugin.id, valid=True, score=score, embedding=embedding1
    )

    item2 = await create_item()
    score2 = 3.42
    embedding2 = [0.4, 0.5, 0.6]

    result_data = [
        {'item_id': item1.id, 'valid': True, 'score': score2, 'embedding': embedding2}, # test update
        {'item_id': item2.id, 'valid': True, 'score': score2, 'embedding': embedding2},
    ]

    result_records = await result_checkin(result_data, plugin.id)
    assert result_records[0].score == score2
    assert result_records[0].embedding == embedding2

