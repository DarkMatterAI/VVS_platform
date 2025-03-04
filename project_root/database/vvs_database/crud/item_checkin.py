from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import List, Dict

from vvs_database.models import Item, ItemSource, ItemResult, Assembly, AssemblyComponent
from vvs_database import schemas 

async def item_checkin(db: AsyncSession, new_items: List[schemas.NewItem], plugin_id: int) -> Dict:
    """Check in multiple items, creating or updating them as needed."""
    # Create a list of unique items
    unique_items = {ni.item: ni.external_id for ni in new_items}

    # Create the insert instance for the Item table
    ins_stmt = pg_insert(Item)
    item_stmt = ins_stmt.values([{"item": item} for item in unique_items.keys()])
    item_stmt = item_stmt.on_conflict_do_update(
        index_elements=["item"],
        set_={"item": item_stmt.excluded.item}
    ).returning(Item.id, Item.item, Item.created_at)

    result = await db.execute(item_stmt)
    item_records = result.fetchall()

    # Prepare data for ItemSource using the returned item IDs
    item_source_data = []
    for item in item_records:
        external_id = unique_items[item.item]
        external_id = external_id if external_id is None else str(external_id)
        item_source_data.append({
            "item_id": item.id,
            "external_id": external_id,
            "plugin_id": plugin_id,
        })
    
    # Similarly, create the insert instance for the ItemSource table
    ins_stmt_source = pg_insert(ItemSource)
    source_stmt = ins_stmt_source.values(item_source_data)
    source_stmt = source_stmt.on_conflict_do_update(
        index_elements=["item_id", "plugin_id"],
        set_={"external_id": source_stmt.excluded.external_id}
    ).returning(
        ItemSource.item_id,
        ItemSource.external_id,
        ItemSource.plugin_id,
        ItemSource.created_at
    )

    result = await db.execute(source_stmt)
    item_source_records = result.fetchall()

    await db.commit()

    item_records_dict = {i.item: i for i in item_records}
    item_source_records_dict = {i.item_id: i for i in item_source_records}

    item_records_response = [item_records_dict[ni.item] for ni in new_items]
    item_source_records_response = [item_source_records_dict[i.id] for i in item_records_response]
    
    return {
        "items": item_records_response,
        "item_sources": item_source_records_response
    }

async def result_checkin(
    db: AsyncSession, 
    new_results: List[schemas.NewResult], 
    plugin_id: int
) -> List[ItemResult]:
    """Check in multiple results, creating or updating them as needed."""
    unique_results = {nr.item_id: nr for nr in new_results}

    values = [
        {
            "item_id": result.item_id,
            "plugin_id": plugin_id,
            "valid": result.valid,
            "score": result.score,
            "embedding": result.embedding,
        }
        for result in unique_results.values()
    ]
    
    stmt = pg_insert(ItemResult).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[ItemResult.item_id, ItemResult.plugin_id],
        set_={
            'valid': stmt.excluded.valid,
            'score': stmt.excluded.score,
            'embedding': stmt.excluded.embedding,
            'created_at': func.now()
        }
    ).returning(
        ItemResult.item_id,
        ItemResult.plugin_id,
        ItemResult.valid,
        ItemResult.score,
        ItemResult.embedding,
        ItemResult.created_at
    )
    
    result = await db.execute(stmt)
    result_records = result.fetchall()
    await db.commit()

    result_dict = {i.item_id: i for i in result_records}
    result = [result_dict[nr.item_id] for nr in new_results]

    return result


async def assembly_checkin(db: AsyncSession, 
                          new_assemblies: List[schemas.NewAssembly], 
                          plugin_id: int) -> Dict:
    """
    Check in multiple assemblies, creating or updating them as needed.
    
    Optimized implementation with bulk operations for assembly creation.
    """
    # Step 1: Check in the assembly result items (already optimized)
    # new_items = [schemas.NewItem(item=a.item, external_id=a.external_id) for a in new_assemblies]
    # item_checkin_result = await item_checkin(db, new_items, plugin_id)
    item_checkin_result = await item_checkin(db, new_assemblies, plugin_id)
    checked_in_items = item_checkin_result["items"]
    
    # Create a mapping from item string to item ID for easy lookup
    item_map = {item.item: item.id for item in checked_in_items}
    
    # Step 2: Generate assembly and component data for bulk operations
    assembly_data = []
    # component_data = []
    assembly_keys = []
    
    # Temporary mapping to link assemblies with their components
    assembly_to_components = {}
    
    for i, new_assembly in enumerate(new_assemblies):
        product_id = item_map[new_assembly.item]
        
        # Sort components by assembly index
        sorted_components = sorted(new_assembly.components, key=lambda x: x.assembly_index)
        component_ids = [comp.item_id for comp in sorted_components]
        
        # Generate assembly key
        assembly_key = f"{plugin_id}_{'_'.join(map(str, component_ids))}_{product_id}"
        assembly_keys.append(assembly_key)

        # Generate component key
        component_key = f"{plugin_id}_{'_'.join(map(str, component_ids))}"
        
        # Store data for bulk creation
        assembly_data.append({
            "product_id": product_id,
            "plugin_id": plugin_id,
            "assembly_key": assembly_key,
            "component_key": component_key
        })
        
        # Store component data with placeholder for assembly_id
        assembly_to_components[assembly_key] = [
            {
                "assembly_index": comp.assembly_index,
                "component_id": comp.item_id
            }
            for comp in new_assembly.components
        ]
    
    # Step 3: Find existing assemblies to avoid duplicates
    result = await db.execute(
        select(Assembly)
        .where(Assembly.assembly_key.in_(assembly_keys))
    )
    existing_assemblies = result.scalars().all()
    existing_keys = {a.assembly_key: a for a in existing_assemblies}
    
    # Step 4: Insert new assemblies (those not in existing_keys)
    new_assembly_data = [
        data for i, data in enumerate(assembly_data) 
        if assembly_keys[i] not in existing_keys
    ]
    new_assembly_keys = [
        key for key in assembly_keys 
        if key not in existing_keys
    ]
    
    inserted_assemblies = []
    if new_assembly_data:
        stmt = pg_insert(Assembly).values(new_assembly_data)
        stmt = stmt.returning(Assembly.assembly_id, Assembly.assembly_key)
        result = await db.execute(stmt)
        inserted_assemblies = result.fetchall()
        
    # Create mapping of all assembly keys to assembly IDs
    all_assembly_mapping = {**{a.assembly_key: a.assembly_id for a in existing_assemblies}}
    for assembly in inserted_assemblies:
        all_assembly_mapping[assembly.assembly_key] = assembly.assembly_id
    
    # Step 5: Insert components for new assemblies
    for assembly_key in new_assembly_keys:
        assembly_id = all_assembly_mapping[assembly_key]
        components = assembly_to_components[assembly_key]
        
        component_insert_data = [
            {
                "assembly_id": assembly_id,
                "assembly_index": comp["assembly_index"],
                "component_id": comp["component_id"]
            }
            for comp in components
        ]
        
        if component_insert_data:
            await db.execute(
                pg_insert(AssemblyComponent).values(component_insert_data)
            )
    
    # Step 6: Load all assemblies with their components for the response
    result = await db.execute(
        select(Assembly)
        .options(selectinload(Assembly.components))
        .where(Assembly.assembly_key.in_(assembly_keys))
    )
    all_assemblies = result.scalars().all()
    
    # Map to original order for the response
    assembly_mapping = {a.assembly_key: a for a in all_assemblies}
    assemblies_result = [assembly_mapping[key] for key in assembly_keys]
    
    await db.commit()
    
    return {
        "items": checked_in_items,
        "item_sources": item_checkin_result["item_sources"],
        "assemblies": assemblies_result
    }
