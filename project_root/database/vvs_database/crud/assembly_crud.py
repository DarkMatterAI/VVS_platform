from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import Optional, List

from vvs_database.models import Assembly, AssemblyComponent

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
