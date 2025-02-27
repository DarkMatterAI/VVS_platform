import pytest 

@pytest.mark.asyncio
async def test_assembly_create(create_item, create_test_assembly_plugin, create_assembly):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin(num_parents=2)
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly = await create_assembly(plugin.id, product.id, component_data)
    
    # Verify assembly
    assert assembly.plugin_id == plugin.id
    assert assembly.product_id == product.id
    assert len(assembly.components) == 2
    
    # Verify components are in the right order
    sorted_components = sorted(assembly.components, key=lambda x: x.assembly_index)
    assert sorted_components[0].component_id == component1.id
    assert sorted_components[1].component_id == component2.id

@pytest.mark.asyncio
async def test_assembly_get_by_id(create_item, create_test_assembly_plugin, 
                               create_assembly, get_assembly_by_id):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly = await create_assembly(plugin.id, product.id, component_data)
    
    # Get assembly by ID
    retrieved_assembly = await get_assembly_by_id(assembly.assembly_id)
    
    # Verify assembly
    assert retrieved_assembly is not None
    assert retrieved_assembly.assembly_id == assembly.assembly_id
    assert retrieved_assembly.plugin_id == plugin.id
    assert retrieved_assembly.product_id == product.id
    assert len(retrieved_assembly.components) == 2

@pytest.mark.asyncio
async def test_assembly_get_by_product_plugin(create_item, create_test_assembly_plugin, 
                                         create_assembly, get_assembly_by_product_plugin):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly = await create_assembly(plugin.id, product.id, component_data)
    
    # Get assembly by product and plugin
    retrieved_assembly = await get_assembly_by_product_plugin(product.id, plugin.id)
    
    # Verify assembly
    assert retrieved_assembly is not None
    assert retrieved_assembly.assembly_id == assembly.assembly_id
    assert retrieved_assembly.plugin_id == plugin.id
    assert retrieved_assembly.product_id == product.id

@pytest.mark.asyncio
async def test_assembly_get_by_component(create_item, create_test_assembly_plugin, 
                                     create_assembly, get_assemblies_by_component):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly = await create_assembly(plugin.id, product.id, component_data)
    
    # Get assemblies by component
    assemblies1 = await get_assemblies_by_component(component1.id)
    assemblies2 = await get_assemblies_by_component(component2.id)
    
    # Verify assemblies
    assert len(assemblies1) == 1
    assert assemblies1[0].assembly_id == assembly.assembly_id
    
    assert len(assemblies2) == 1
    assert assemblies2[0].assembly_id == assembly.assembly_id

@pytest.mark.asyncio
async def test_assembly_continuous_indices_validation(create_item, create_test_assembly_plugin, create_assembly):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    component3 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin(num_parents=3)
    
    # Create component data with non-continuous indices
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 2, "component_id": component2.id},  # Missing index 1
        {"assembly_index": 3, "component_id": component3.id}
    ]
    
    # Attempt to create assembly should fail
    with pytest.raises(ValueError) as excinfo:
        await create_assembly(plugin.id, product.id, component_data)
    
    assert "Assembly indices must be continuous" in str(excinfo.value)
    
    # Fix the indices and try again
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id},
        {"assembly_index": 2, "component_id": component3.id}
    ]
    
    # This should succeed
    assembly = await create_assembly(plugin.id, product.id, component_data)
    assert len(assembly.components) == 3

@pytest.mark.asyncio
async def test_assembly_delete(create_item, create_test_assembly_plugin, 
                           create_assembly, delete_assembly, get_assembly_by_id):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly = await create_assembly(plugin.id, product.id, component_data)
    
    # Delete assembly
    await delete_assembly(assembly)
    
    # Verify assembly is deleted
    deleted_assembly = await get_assembly_by_id(assembly.assembly_id)
    assert deleted_assembly is None

@pytest.mark.asyncio
async def test_assembly_deduplication(create_item, create_test_assembly_plugin, create_assembly):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly1 = await create_assembly(plugin.id, product.id, component_data)
    
    # Create identical assembly
    assembly2 = await create_assembly(plugin.id, product.id, component_data)
    
    # Verify both references are the same assembly
    assert assembly1.assembly_id == assembly2.assembly_id
    assert assembly1.assembly_key == assembly2.assembly_key

@pytest.mark.asyncio
async def test_product_delete_propagation(create_item, create_test_assembly_plugin, 
                                      create_assembly, get_assembly_by_id, delete_item):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()
    
    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    
    # Create assembly
    assembly = await create_assembly(plugin.id, product.id, component_data)
    
    # Delete product
    await delete_item(product)
    
    # Verify assembly is deleted due to cascade
    deleted_assembly = await get_assembly_by_id(assembly.assembly_id)
    assert deleted_assembly is None

