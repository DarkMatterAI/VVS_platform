import pytest 

from vvs_database import crud 

@pytest.mark.asyncio
async def test_assembly_create(db_session, create_item, create_test_assembly_plugin):
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
    assembly = await crud.create_assembly(db_session, plugin.id, product.id, component_data)
    
    # Verify assembly
    assert assembly.plugin_id == plugin.id
    assert assembly.product_id == product.id
    assert len(assembly.components) == 2
    
    # Verify components are in the right order
    sorted_components = sorted(assembly.components, key=lambda x: x.assembly_index)
    assert sorted_components[0].component_id == component1.id
    assert sorted_components[1].component_id == component2.id

@pytest.mark.asyncio
async def test_assembly_get_by_id(db_session, 
                                  create_item, 
                                  create_test_assembly_plugin, 
                                  get_assembly_by_id):
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
    assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
    
    # Get assembly by ID
    retrieved_assembly = await get_assembly_by_id(assembly.assembly_id)
    
    # Verify assembly
    assert retrieved_assembly is not None
    assert retrieved_assembly.assembly_id == assembly.assembly_id
    assert retrieved_assembly.plugin_id == plugin.id
    assert retrieved_assembly.product_id == product.id
    assert len(retrieved_assembly.components) == 2

@pytest.mark.asyncio
async def test_assembly_get_by_product_plugin(db_session, 
                                              create_item, 
                                              create_test_assembly_plugin, 
                                              get_assembly_by_product_plugin):
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
    assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
    
    # Get assembly by product and plugin
    retrieved_assembly = await get_assembly_by_product_plugin(product.id, plugin.id)
    
    # Verify assembly
    assert retrieved_assembly is not None
    assert retrieved_assembly.assembly_id == assembly.assembly_id
    assert retrieved_assembly.plugin_id == plugin.id
    assert retrieved_assembly.product_id == product.id

@pytest.mark.asyncio
async def test_assembly_get_by_component(db_session, 
                                         create_item, 
                                         create_test_assembly_plugin, 
                                         get_assemblies_by_component):
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
    assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
    
    # Get assemblies by component
    assemblies1 = await get_assemblies_by_component(component1.id)
    assemblies2 = await get_assemblies_by_component(component2.id)
    
    # Verify assemblies
    assert len(assemblies1) == 1
    assert assemblies1[0].assembly_id == assembly.assembly_id
    
    assert len(assemblies2) == 1
    assert assemblies2[0].assembly_id == assembly.assembly_id

@pytest.mark.asyncio
async def test_assembly_get_by_component_key(db_session, 
                                             create_item, 
                                             create_test_assembly_plugin, 
                                             get_assemblies_by_component_key):
    # Create components and product
    component1 = await create_item()
    component2 = await create_item()
    product1 = await create_item()
    product2 = await create_item()
    
    # Create assembly plugin
    plugin = await create_test_assembly_plugin()

    # Create component data
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id}
    ]
    component_key = f"{plugin.id}_{component1.id}_{component2.id}"

    # create assembly
    assembly_ids = []
    for product in [product1, product2]:
        assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
        assembly_ids.append(assembly.assembly_id)
        assert assembly.component_key == component_key

    assemblies = await get_assemblies_by_component_key(component_key)
    assert len(assemblies) == 2
    for assembly in assemblies:
        assert assembly.assembly_id in assembly_ids

@pytest.mark.asyncio
async def test_assembly_get_by_component_keys(db_session, 
                                              create_item, 
                                              create_test_assembly_plugin,
                                              get_assemblies_by_component_keys):
    n_items = 3
    plugin = await create_test_assembly_plugin()

    assemblies = []
    for i in range(n_items):
        component1 = await create_item()
        component2 = await create_item()
        product = await create_item()

        component_data = [
            {"assembly_index": 0, "component_id": component1.id},
            {"assembly_index": 1, "component_id": component2.id}
        ]
        assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
        assemblies.append(assembly)

    component_keys = [i.component_key for i in assemblies]
    assembly_ids = [i.assembly_id for i in assemblies]

    assembly_records = await get_assemblies_by_component_keys(component_keys)
    assert len(assembly_records) == len(assemblies)
    for record in assembly_records:
        assert record.assembly_id in assembly_ids 


@pytest.mark.asyncio
async def test_assembly_continuous_indices_validation(db_session, 
                                                      create_item, 
                                                      create_test_assembly_plugin):
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
        assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
    
    assert "Assembly indices must be continuous" in str(excinfo.value)
    
    # Fix the indices and try again
    component_data = [
        {"assembly_index": 0, "component_id": component1.id},
        {"assembly_index": 1, "component_id": component2.id},
        {"assembly_index": 2, "component_id": component3.id}
    ]
    
    # This should succeed
    assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
    assert len(assembly.components) == 3

@pytest.mark.asyncio
async def test_assembly_delete(db_session, 
                               create_item, 
                               create_test_assembly_plugin, 
                               get_assembly_by_id):
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
    assembly = await crud.create_assembly(db_session, plugin.id, 
                                          product.id, component_data)
    
    # Delete assembly
    result = await crud.delete_assembly(db_session, assembly)
    
    # Verify assembly is deleted
    deleted_assembly = await get_assembly_by_id(assembly.assembly_id)
    assert deleted_assembly is None

@pytest.mark.asyncio
async def test_assembly_deduplication(db_session, 
                                      create_item, 
                                      create_test_assembly_plugin):
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
    assembly1 = await crud.create_assembly(db_session, plugin.id, product.id, component_data)
    
    # Create identical assembly
    assembly2 = await crud.create_assembly(db_session, plugin.id, product.id, component_data)
    
    # Verify both references are the same assembly
    assert assembly1.assembly_id == assembly2.assembly_id
    assert assembly1.assembly_key == assembly2.assembly_key

@pytest.mark.asyncio
async def test_product_delete_propagation(db_session, 
                                          create_item, 
                                          create_test_assembly_plugin, 
                                          get_assembly_by_id,):
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
    assembly = await crud.create_assembly(db_session, plugin.id, product.id, component_data)
    
    # Delete product
    _ = await crud.delete_item(db_session, product)
    
    # Verify assembly is deleted due to cascade
    deleted_assembly = await get_assembly_by_id(assembly.assembly_id)
    assert deleted_assembly is None

