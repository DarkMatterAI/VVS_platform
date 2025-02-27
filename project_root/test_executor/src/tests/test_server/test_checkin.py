import os 
import pytest 
import numpy as np 

from tests.utils import fetch_test_api_plugins, get_request_data
from tests.test_helpers import (
    execute_plugin_helper
)

EMBEDDING_SIZE = int(os.environ.get('TEST_EMBEDDING_SIZE', 32))

@pytest.mark.asyncio
async def test_embedding_checkin_integration(backend_client, get_item_result, create_test_item):
    """Test embedding plugin execution with result check-in."""    
    # Create a test item
    item = await create_test_item()
    
    # Get test plugins
    plugins = fetch_test_api_plugins(backend_client, plugin_type='embedding')
    plugin = plugins[0]
    
    # Create custom request with our test item
    custom_request = {
        "request_data": get_request_data(plugin, item_id=item.id),
        "item_data": {
            "item_id": item.id,
            "item": item.item,
            "external_id": None
        }
    }
    
    # Execute plugin with checkin_result=True
    result = execute_plugin_helper(
        backend_client, 
        plugins, 
        'embedding', 
        custom_request=custom_request,
        checkin_result=True
    )
    
    # Verify the result was checked in to the database
    item_result = await get_item_result(item.id, plugin["id"])
    
    assert item_result is not None
    assert item_result.valid == result["valid"]
    if "embedding" in result and result["embedding"]:
        assert item_result.embedding == result["embedding"]


@pytest.mark.asyncio
async def test_filter_checkin_integration(backend_client, get_item_result, create_test_item):
    """Test filter plugin execution with result check-in."""    
    # Create a test item
    item = await create_test_item()
    
    # Get test plugins
    plugins = fetch_test_api_plugins(backend_client, plugin_type='filter')
    plugin = plugins[0]
    
    # Create custom request with our test item
    custom_request = {
        "request_data": get_request_data(plugin, item_id=item.id),
        "item_data": {
            "item_id": item.id,
            "item": item.item,
            "external_id": None
        }
    }
    
    # Execute plugin with checkin_result=True
    result = execute_plugin_helper(
        backend_client, 
        plugins, 
        'filter', 
        custom_request=custom_request,
        checkin_result=True
    )
    
    # Verify the result was checked in to the database
    item_result = await get_item_result(item.id, plugin["id"])
    
    assert item_result is not None
    assert item_result.valid == result["valid"]
    assert item_result.score is None  # Filter plugins don't set score
    assert item_result.embedding is None 

@pytest.mark.asyncio
async def test_score_checkin_integration(backend_client, get_item_result, create_test_item):
    """Test score plugin execution with result check-in."""    
    # Create a test item
    item = await create_test_item()
    
    # Get test plugins
    plugins = fetch_test_api_plugins(backend_client, plugin_type='score')
    plugin = plugins[0]
    
    # Create custom request with our test item
    custom_request = {
        "request_data": get_request_data(plugin, item_id=item.id),
        "item_data": {
            "item_id": item.id,
            "item": item.item,
            "external_id": None
        }
    }
    
    # Execute plugin with checkin_result=True
    result = execute_plugin_helper(
        backend_client, 
        plugins, 
        'score', 
        custom_request=custom_request,
        checkin_result=True
    )
    
    # Verify the result was checked in to the database
    item_result = await get_item_result(item.id, plugin["id"])
    
    assert item_result is not None
    assert item_result.valid == result["valid"]
    assert item_result.score == result["score"]
    assert item_result.embedding is None 

@pytest.mark.asyncio
async def test_data_source_checkin_integration(backend_client, get_item_source, get_item_by_name):
    """Test data source plugin execution with result check-in."""    
    # Get test plugins
    plugins = fetch_test_api_plugins(backend_client, plugin_type='data_source')
    embed_plugins = fetch_test_api_plugins(backend_client, plugin_type='embedding')
    
    plugin = plugins[0]
    embed_plugin = embed_plugins[0]
    
    # Create custom request
    custom_request = {
        "request_data": get_request_data(plugin),
        "embedding": {
            "plugin_id": embed_plugin["id"],
            "plugin_name": embed_plugin["name"],
            "embedding": np.random.randn(EMBEDDING_SIZE).tolist()
        },
        "k": 5
    }
    
    # Execute plugin with checkin_result=True
    result = execute_plugin_helper(
        backend_client, 
        plugins, 
        'data_source', 
        custom_request=custom_request,
        checkin_result=True
    )
    
    # Verify that the items were checked in
    assert result["valid"]
    assert len(result["result"]) > 0
    
    # Check that at least one item was stored in the database
    first_item = result["result"][0]
    # item = await crud.get_item_by_name(db_session, first_item["item"])
    item = await get_item_by_name(first_item['item'])
    
    assert item is not None
    
    # Check that the item source was created
    item_source = await get_item_source(item.id, plugin['id'])
    # item_source = await crud.get_item_source(db_session, item.id, plugin["id"])
    assert item_source is not None