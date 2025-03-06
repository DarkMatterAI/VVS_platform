import pytest 

from vvs_database import crud 

@pytest.mark.asyncio
async def test_assembly_checkin_basic(assembly_checkin, create_item, create_test_assembly_plugin):
    # Create components that will be part of the assembly
    component1 = await create_item()
    component2 = await create_item()
    
    # Create an assembly plugin
    plugin = await create_test_assembly_plugin(num_parents=2)
    
    # Prepare assembly data
    assembly_data = [
        {
            "item": "Assembly Result 1",
            "external_id": "ext-assembly-1",
            "components": [
                {"item_id": component1.id, "assembly_index": 0},
                {"item_id": component2.id, "assembly_index": 1}
            ]
        }
    ]
    
    # Execute assembly checkin
    result = await assembly_checkin(assembly_data, plugin.id)
    
    # Verify the results
    assemblies = result["assemblies"]
    assert len(assemblies) == 1
    
    assembly = assemblies[0]
    assert assembly.product_id == result["items"][0].id
    assert assembly.plugin_id == plugin.id
    
    # Verify components
    components = sorted(assembly.components, key=lambda x: x.assembly_index)
    assert len(components) == 2
    assert components[0].component_id == component1.id
    assert components[1].component_id == component2.id

@pytest.mark.asyncio
async def test_assembly_checkin_multiple(assembly_checkin, create_item, create_test_assembly_plugin):
    # Create components
    component1 = await create_item()
    component2 = await create_item()
    component3 = await create_item()
    
    # Create an assembly plugin
    plugin = await create_test_assembly_plugin(num_parents=3)
    
    # Prepare assembly data for multiple assemblies
    assembly_data = [
        {
            "item": "Assembly Result A",
            "external_id": "ext-assembly-a",
            "components": [
                {"item_id": component1.id, "assembly_index": 0},
                {"item_id": component2.id, "assembly_index": 1}
            ]
        },
        {
            "item": "Assembly Result B",
            "external_id": "ext-assembly-b",
            "components": [
                {"item_id": component2.id, "assembly_index": 0},
                {"item_id": component3.id, "assembly_index": 1}
            ]
        }
    ]
    
    # Execute assembly checkin
    result = await assembly_checkin(assembly_data, plugin.id)
    
    # Verify the results
    assemblies = result["assemblies"]
    assert len(assemblies) == 2
    
    # First assembly
    assert assemblies[0].product_id == result["items"][0].id
    assert assemblies[0].plugin_id == plugin.id
    
    # Second assembly
    assert assemblies[1].product_id == result["items"][1].id
    assert assemblies[1].plugin_id == plugin.id
    
    # Components for first assembly
    components1 = sorted(assemblies[0].components, key=lambda x: x.assembly_index)
    assert len(components1) == 2
    assert components1[0].component_id == component1.id
    assert components1[1].component_id == component2.id
    
    # Components for second assembly
    components2 = sorted(assemblies[1].components, key=lambda x: x.assembly_index)
    assert len(components2) == 2
    assert components2[0].component_id == component2.id
    assert components2[1].component_id == component3.id

@pytest.mark.asyncio
async def test_assembly_checkin_deduplication(db_session,
                                              assembly_checkin, 
                                              create_item, 
                                              create_test_assembly_plugin):
    # Create components
    component1 = await create_item()
    component2 = await create_item()
    
    # Create an assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Prepare identical assembly data (same components, same order)
    assembly_data = [
        {
            "item": "Duplicate Assembly",
            "external_id": "ext-duplicate-1",
            "components": [
                {"item_id": component1.id, "assembly_index": 0},
                {"item_id": component2.id, "assembly_index": 1}
            ]
        }
    ]
    
    # First check-in
    result1 = await assembly_checkin(assembly_data, plugin.id)
    assembly1_id = result1["assemblies"][0].assembly_id
    
    # Second check-in with the same data
    result2 = await assembly_checkin(assembly_data, plugin.id)
    assembly2_id = result2["assemblies"][0].assembly_id
    
    # Verify that both check-ins resulted in the same assembly
    assert assembly1_id == assembly2_id
    
    # Get the assembly to verify
    assembly = await crud.get_assembly_by_id(db_session, assembly1_id)
    assert assembly is not None
    assert len(assembly.components) == 2

@pytest.mark.asyncio
async def test_assembly_checkin_same_components_different_order(assembly_checkin, create_item, 
                                                                create_test_assembly_plugin):
    # Create components
    component1 = await create_item()
    component2 = await create_item()
    
    # Create an assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # First assembly with components in one order
    assembly_data1 = [
        {
            "item": "Assembly Order 1",
            "external_id": "ext-order-1",
            "components": [
                {"item_id": component1.id, "assembly_index": 0},
                {"item_id": component2.id, "assembly_index": 1}
            ]
        }
    ]
    
    # Second assembly with same components in different order
    assembly_data2 = [
        {
            "item": "Assembly Order 2",
            "external_id": "ext-order-2",
            "components": [
                {"item_id": component2.id, "assembly_index": 0},
                {"item_id": component1.id, "assembly_index": 1}
            ]
        }
    ]
    
    # Check in both assemblies
    result1 = await assembly_checkin(assembly_data1, plugin.id)
    result2 = await assembly_checkin(assembly_data2, plugin.id)
    
    # Verify that they are different assemblies
    assembly1_id = result1["assemblies"][0].assembly_id
    assembly2_id = result2["assemblies"][0].assembly_id
    
    assert assembly1_id != assembly2_id

@pytest.mark.asyncio
async def test_assembly_checkin_existing_item(assembly_checkin, create_item, create_test_assembly_plugin):
    # Create components and an existing product item
    component1 = await create_item()
    component2 = await create_item()
    existing_product = await create_item("Existing Product")
    
    # Create an assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Prepare assembly data using the existing product
    assembly_data = [
        {
            "item": existing_product.item,  # use the existing item's name
            "external_id": "ext-existing",
            "components": [
                {"item_id": component1.id, "assembly_index": 0},
                {"item_id": component2.id, "assembly_index": 1}
            ]
        }
    ]
    
    # Execute assembly checkin
    result = await assembly_checkin(assembly_data, plugin.id)
    
    # Verify the results
    assemblies = result["assemblies"]
    assert len(assemblies) == 1
    
    # Verify the product ID matches the existing item
    assert result["items"][0].id == existing_product.id

@pytest.mark.asyncio
async def test_assembly_checkin_with_varying_component_counts(assembly_checkin, create_item, create_test_assembly_plugin):
    # Create components
    components = [await create_item() for _ in range(5)]
    
    # Create an assembly plugin
    plugin = await create_test_assembly_plugin(num_parents=5)
    
    # Prepare assembly data with different component counts
    assembly_data = [
        {
            "item": "Assembly with 2 components",
            "external_id": "ext-2comp",
            "components": [
                {"item_id": components[0].id, "assembly_index": 0},
                {"item_id": components[1].id, "assembly_index": 1}
            ]
        },
        {
            "item": "Assembly with 3 components",
            "external_id": "ext-3comp",
            "components": [
                {"item_id": components[0].id, "assembly_index": 0},
                {"item_id": components[1].id, "assembly_index": 1},
                {"item_id": components[2].id, "assembly_index": 2}
            ]
        },
        {
            "item": "Assembly with 5 components",
            "external_id": "ext-5comp",
            "components": [
                {"item_id": components[0].id, "assembly_index": 0},
                {"item_id": components[1].id, "assembly_index": 1},
                {"item_id": components[2].id, "assembly_index": 2},
                {"item_id": components[3].id, "assembly_index": 3},
                {"item_id": components[4].id, "assembly_index": 4}
            ]
        }
    ]
    
    # Execute assembly checkin
    result = await assembly_checkin(assembly_data, plugin.id)
    
    # Verify the results
    assemblies = result["assemblies"]
    assert len(assemblies) == 3
    
    # Verify component counts
    assert len(assemblies[0].components) == 2
    assert len(assemblies[1].components) == 3
    assert len(assemblies[2].components) == 5
