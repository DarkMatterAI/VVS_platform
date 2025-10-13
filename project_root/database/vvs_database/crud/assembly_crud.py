from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, exists, and_, true
from sqlalchemy.orm import selectinload
from typing import Optional, List, Dict

from vvs_database.models.item_models import Item, ItemSource, ItemResult, Assembly, AssemblyComponent
from vvs_database.models.plugin_models import AssemblyPlugin

async def create_assembly(
    db: AsyncSession,
    plugin_id: int,
    product_id: int,
    component_data: List[dict]
) -> Assembly:
    """
    Create a new assembly with components.
    
    Args:
        db: Database session
        plugin_id: ID of the assembly plugin
        product_id: ID of the product item
        component_data: List of dicts with component_id and assembly_index
        
    Returns:
        The created assembly
    """
    # Sort components by assembly_index
    sorted_components = sorted(component_data, key=lambda x: x["assembly_index"])
    
    # Validate that assembly_index values are continuous starting from 0
    expected_indices = range(len(sorted_components))
    actual_indices = [comp["assembly_index"] for comp in sorted_components]
    
    if list(actual_indices) != list(expected_indices):
        raise ValueError(
            f"Assembly indices must be continuous starting from 0. "
            f"Got {actual_indices}, expected {list(expected_indices)}"
        )
    
    component_ids = [comp["component_id"] for comp in sorted_components]
    
    # Use get_or_create to avoid duplicates
    assembly = await Assembly.get_or_create(db, product_id, plugin_id, component_ids)
    await db.commit()
    
    return assembly

async def get_assembly_by_id(
    db: AsyncSession,
    assembly_id: int
) -> Optional[Assembly]:
    """Get an assembly by ID."""
    result = await db.execute(
        select(Assembly)
        .options(selectinload(Assembly.components))
        .filter(Assembly.assembly_id == assembly_id)
    )
    return result.scalar_one_or_none()

async def get_assembly_by_product_plugin(
    db: AsyncSession,
    product_id: int,
    plugin_id: int
) -> Optional[Assembly]:
    """Get an assembly by product ID and plugin ID."""
    result = await db.execute(
        select(Assembly)
        .options(selectinload(Assembly.components))
        .filter(
            Assembly.product_id == product_id,
            Assembly.plugin_id == plugin_id
        )
    )
    return result.scalar_one_or_none()

async def get_assemblies_by_component(
    db: AsyncSession,
    component_id: int
) -> List[Assembly]:
    """Get all assemblies that use a specific component."""
    result = await db.execute(
        select(Assembly)
        .options(selectinload(Assembly.components))
        .join(AssemblyComponent)
        .filter(AssemblyComponent.component_id == component_id)
        .distinct()
    )
    return result.scalars().all()

async def get_assemblies_by_component_key(
    db: AsyncSession,
    component_key: str
) -> List[Assembly]:
    """Get all assemblies with a specific component key"""
    result = await db.execute(
        select(Assembly)
        .options(selectinload(Assembly.components))
        .filter(Assembly.component_key == component_key)
        .distinct()
    )
    return result.scalars().all()

async def get_assemblies_by_component_keys(
    db: AsyncSession,
    component_keys: List[str]
) -> List[Assembly]:
    """Get all assemblies with any of the provided component keys"""
    # async with db.begin():
    result = await db.execute(
        select(Assembly)
        .options(selectinload(Assembly.components))
        .options(selectinload(Assembly.product))
        .where(Assembly.component_key.in_(component_keys))
    )
    all_assemblies = result.scalars().all()
    return all_assemblies

async def delete_assembly(
    db: AsyncSession,
    assembly: Assembly
) -> Assembly:
    """Delete an assembly and its components."""
    await db.delete(assembly)
    await db.commit()
    return assembly

async def delete_assemblies_with_component(
    db: AsyncSession,
    component_item_id: int,
) -> int:
    """
    Delete every Assembly that references *component_item_id* in AssemblyComponent.
    Returns the number of Assembly rows deleted.
    """
    del_stmt = (
        delete(Assembly)
        .where(
            Assembly.assembly_id.in_(
                select(AssemblyComponent.assembly_id).where(
                    AssemblyComponent.component_id == component_item_id
                )
            )
        )
        .returning(Assembly.assembly_id)
    )
    result = await db.execute(del_stmt)
    deleted_ids = result.scalars().all()
    await db.commit()
    return len(deleted_ids)

async def prune_orphan_assembly_products(
    session: AsyncSession,
    *,
    assembly_plugin_ids: Optional[List[int]] = None,
) -> Dict[str, int]:
    """
    Remove item-level artifacts left after assembly pruning:
      - Delete ItemResult rows for product items that are no longer products of any Assembly
        (optionally restricted to Items whose ItemSource came from specific assembly plugins).
      - Delete ItemSource rows for those orphan product items (for the assembly plugins in scope).
      - Finally, remove any now-unreferenced Items via Item.cleanup_unreferenced().
    Returns counts for each step.
    """

    # Base SELECT of orphan product items:
    # Items that *had* an ItemSource from an AssemblyPlugin, but are *not* a product
    # of any remaining Assembly (optionally require the same plugin_id to match).
    isrc = ItemSource
    asm  = Assembly

    orphan_items_sq = (
        select(isrc.item_id)
        .where(
            # restrict to ItemSource rows created by an AssemblyPlugin
            exists(select(AssemblyPlugin.id).where(AssemblyPlugin.id == isrc.plugin_id)),
            # optionally limit to specific assembly plugins
            (isrc.plugin_id.in_(assembly_plugin_ids) if assembly_plugin_ids else true()),
            # and prove there is NO Assembly referencing this item as product
            ~exists(
                select(1).where(
                    and_(
                        asm.product_id == isrc.item_id,
                        (asm.plugin_id == isrc.plugin_id) if assembly_plugin_ids else true(),
                    )
                )
            ),
        )
        .distinct()
        .subquery()
    )

    # 1) Delete ItemResult rows for these items (any plugin)
    del_results_stmt = (
        delete(ItemResult)
        .where(ItemResult.item_id.in_(select(orphan_items_sq.c.item_id)))
        .returning(ItemResult.item_id)
    )
    res = await session.execute(del_results_stmt)
    _deleted_result_rows = res.scalars().all()
    results_deleted = len(_deleted_result_rows)

    # 2) Delete ItemSource rows for these items (only the assembly plugins in scope)
    del_sources_stmt = (
        delete(ItemSource)
        .where(
            and_(
                ItemSource.item_id.in_(select(orphan_items_sq.c.item_id)),
                (ItemSource.plugin_id.in_(assembly_plugin_ids) if assembly_plugin_ids else
                 exists(select(AssemblyPlugin.id).where(AssemblyPlugin.id == ItemSource.plugin_id)))
            )
        )
        .returning(ItemSource.item_id)
    )
    res2 = await session.execute(del_sources_stmt)
    _deleted_source_rows = res2.scalars().all()
    sources_deleted = len(_deleted_source_rows)

    # 3) Remove any Items that are now unreferenced anywhere
    items_deleted = await Item.cleanup_unreferenced(session)

    return {
        "item_results_deleted": results_deleted,
        "item_sources_deleted": sources_deleted,
        "items_deleted": items_deleted,
    }