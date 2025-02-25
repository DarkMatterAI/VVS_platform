from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload, undefer_group, aliased
from sqlalchemy import insert, delete, func, and_
from typing import List, Optional, Dict, Any, Tuple

from vvs_database.exceptions import ValidationError, NotFoundError, ReferenceError
from vvs_database.models import (
    Plugin, EmbeddingPlugin, DataSourcePlugin, FilterPlugin, 
    ScorePlugin, MapperPlugin, AssemblyPlugin, plugin_embeddings
)
from vvs_database.schemas.enums import PluginType, PluginClass

# Utility function
def object_as_dict(obj):
    """Convert SQLAlchemy model instance to dictionary."""
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}

async def get_embeddings(db: AsyncSession, embedding_ids: List[int]):
    """Get embedding plugins by their IDs."""
    stmt = select(EmbeddingPlugin).filter(EmbeddingPlugin.id.in_(embedding_ids))
    result = await db.execute(stmt)
    embeddings = result.scalars().all()
    return embeddings

async def validate_embedding_ids(db: AsyncSession, embedding_ids: List[int]) -> None:
    """Validate that all embedding IDs exist in the database."""
    if not embedding_ids:
        return
    
    if len(embedding_ids) != len(set(embedding_ids)):
        raise ValidationError("Duplicate embedding IDs are not allowed")

    valid_embeddings = await get_embeddings(db, embedding_ids)

    if len(valid_embeddings) != len(embedding_ids):
        invalid_ids = set(embedding_ids) - set(e.id for e in valid_embeddings)
        raise ValidationError(f"Invalid embedding IDs: {invalid_ids}")

async def get_plugin(db: AsyncSession, plugin_id: int):
    """Get a plugin by ID with all related data loaded."""
    stmt = (
        select(Plugin)
        .options(
            selectinload(Plugin.embeddings),
            undefer_group('*')
        )
        .filter(Plugin.id == plugin_id)
    )
    result = await db.execute(stmt)
    plugin = result.scalars().first()
    
    if plugin:
        await db.refresh(plugin)
    
    return plugin

def build_filters(model, filter_params):
    """Build SQLAlchemy filters from dictionary of parameters."""
    filters = []
    for key, value in filter_params.items():
        if value is not None:
            if key in ['name', 'group_key']:
                if isinstance(value, str) and '%' in value:
                    filters.append(getattr(model, key).like(value))
                else:
                    filters.append(getattr(model, key) == value)
            elif isinstance(value, (list, tuple)):
                filters.append(getattr(model, key).in_(value))
            else:
                filters.append(getattr(model, key) == value)
    return filters

async def get_plugins(
    db: AsyncSession, 
    filter_params: Dict[str, Any] = None,
    skip: int = 0, 
    limit: int = 100
):
    """Get plugins with filtering, pagination and eager loading."""
    stmt = (
        select(Plugin)
        .options(
            selectinload(Plugin.embeddings),
            undefer_group('*')
        )
    )

    if filter_params:
        filters = build_filters(Plugin, filter_params)
        stmt = stmt.filter(and_(*filters))

    stmt = stmt.offset(skip).limit(limit)

    result = await db.execute(stmt)
    plugins = result.scalars().all()
    
    for plugin in plugins:
        await db.refresh(plugin)
    
    return plugins

def validate_output_order(output_order):
    """Validate that output order has unique indices."""
    ids = [i['index'] for i in output_order]
    if len(set(ids)) != len(ids):
        raise ValidationError(f"Duplicate index values in output order {ids}")

def get_plugin_data_model(plugin_type: PluginType):
    """Get the SQLAlchemy model class for a plugin type."""
    plugin_type_map = {
        PluginType.EMBEDDING: EmbeddingPlugin,
        PluginType.DATA_SOURCE: DataSourcePlugin,
        PluginType.FILTER: FilterPlugin,
        PluginType.SCORE: ScorePlugin,
        PluginType.MAPPER: MapperPlugin,
        PluginType.ASSEMBLY: AssemblyPlugin
    }
    return plugin_type_map[plugin_type]

async def create_plugin(
    db: AsyncSession, 
    plugin_type: PluginType,
    plugin_data: dict,
    embedding_ids: List[int] = None
):
    """Create a new plugin with the given data."""
    plugin_model = get_plugin_data_model(plugin_type)
    
    # Handle mapper plugin special case
    if plugin_type == PluginType.MAPPER:
        if 'output_order' in plugin_data:
            output_order = plugin_data.get('output_order', [])
            validate_output_order(output_order)
            
            input_embedding_id = plugin_data.get('input_embedding_id')
            embedding_ids = [i['embedding_id'] for i in output_order]
            embedding_ids.append(input_embedding_id)
            embedding_ids = list(set(embedding_ids))
            
            await validate_embedding_ids(db, embedding_ids)
    elif embedding_ids:
        await validate_embedding_ids(db, embedding_ids)
    
    # Create the plugin
    db_plugin = plugin_model(**plugin_data)
    db.add(db_plugin)
    await db.flush()
    
    # Add embedding relationships if needed
    if embedding_ids:
        await db.execute(
            insert(plugin_embeddings).values([
                {"plugin_id": db_plugin.id, "embedding_id": embedding_id}
                for embedding_id in embedding_ids
            ])
        )
    
    await db.commit()
    
    # Reload with relationships
    stmt = select(plugin_model).options(selectinload(plugin_model.embeddings)).filter_by(id=db_plugin.id)
    result = await db.execute(stmt)
    db_plugin = result.scalar_one()
    return db_plugin

def embedding_update(db_plugin, update_data):
    """Update embedding plugin specific fields."""
    if 'vector_length' in update_data:
        db_plugin.vector_length = update_data['vector_length']
    if 'distance_metric' in update_data:
        db_plugin.distance_metric = update_data['distance_metric']

def assembly_update(db_plugin, update_data):
    """Update assembly plugin specific fields."""
    if 'num_parents' in update_data:
        db_plugin.num_parents = update_data['num_parents']

async def update_linked_embeddings(db_plugin, update_data, db, key='embedding_ids'):
    """Update plugin-embedding relationships."""
    if key in update_data:
        await validate_embedding_ids(db, update_data[key])
        await db.execute(
            delete(plugin_embeddings).where(plugin_embeddings.c.plugin_id == db_plugin.id)
        )
        
        if update_data[key]:
            await db.execute(
                insert(plugin_embeddings).values([
                    {"plugin_id": db_plugin.id, "embedding_id": embedding_id}
                    for embedding_id in update_data[key]
                ])
            )

async def mapper_update(db_plugin, update_data, db):
    """Update mapper plugin specific fields."""
    new_embeddings = []

    if 'input_embedding_id' in update_data:
        input_embedding_id = update_data['input_embedding_id']
        await validate_embedding_ids(db, [input_embedding_id])
        new_embeddings.append(input_embedding_id)
        db_plugin.input_embedding_id = update_data['input_embedding_id']
    else:
        new_embeddings.append(db_plugin.input_embedding_id)

    if 'output_order' in update_data:
        output_order = update_data['output_order']
        if len(output_order) < 2:
            raise ValidationError("Must have at least two output embeddings")
        validate_output_order(update_data['output_order'])
        embedding_ids = list(set([i['embedding_id'] for i in output_order]))
        await validate_embedding_ids(db, embedding_ids)
        new_embeddings += embedding_ids 
        db_plugin.output_order = output_order
    else:
        new_embeddings += [i['embedding_id'] for i in db_plugin.output_order]

    new_embeddings = list(set(new_embeddings))
    update_data['new_embedding_links'] = new_embeddings 
    await update_linked_embeddings(db_plugin, update_data, db, key='new_embedding_links')
    update_data.pop('new_embedding_links')

async def update_plugin(db: AsyncSession, plugin_id: int, update_data: dict):
    """Update a plugin with the given data."""
    db_plugin = await get_plugin(db, plugin_id)

    if not db_plugin:
        raise NotFoundError(f"Plugin with ID {plugin_id} not found")
    
    # Update common fields first
    general_fields = ['name', 'timeout', 'max_concurrency', 'max_retries', 
                      'endpoint_url', 'group_key', 'config', 'plugin_metadata']
    for field in general_fields:
        if field in update_data:
            setattr(db_plugin, field, update_data[field])

    # Update specific fields based on plugin type
    if isinstance(db_plugin, EmbeddingPlugin):
        embedding_update(db_plugin, update_data)
    elif isinstance(db_plugin, AssemblyPlugin):
        assembly_update(db_plugin, update_data)
    elif isinstance(db_plugin, (DataSourcePlugin, FilterPlugin, ScorePlugin)):
        await update_linked_embeddings(db_plugin, update_data, db)
    elif isinstance(db_plugin, MapperPlugin):
        await mapper_update(db_plugin, update_data, db)

    await db.commit()
    await db.refresh(db_plugin)

    # Reload to get all association data
    db_plugin = await get_plugin(db, db_plugin.id)
    return db_plugin

async def delete_plugin_from_model(db: AsyncSession, db_plugin: Plugin):
    # Check if embedding plugin is linked to other records
    if isinstance(db_plugin, EmbeddingPlugin):
        linked_plugins = await db.execute(
            select(Plugin)
            .join(plugin_embeddings)
            .filter(plugin_embeddings.c.embedding_id == db_plugin.id)
        )
        linked_plugins = linked_plugins.scalars().all()

        mapper_plugins = await db.execute(
            select(MapperPlugin)
            .filter(MapperPlugin.input_embedding_id == db_plugin.id)
        )

        linked_plugins += mapper_plugins.scalars().all()

        if linked_plugins:
            linked_plugin_names = [p.name for p in linked_plugins]
            raise ReferenceError(
                f"Cannot delete this embedding plugin. It is "
                f"linked to the following plugins: {', '.join(linked_plugin_names)}"
            )

    await db.delete(db_plugin)
    await db.commit()
    return db_plugin

async def delete_plugin(db: AsyncSession, plugin_id: int):
    """Delete a plugin by ID with checks for relationships."""
    db_plugin = await get_plugin(db, plugin_id)
    
    if not db_plugin:
        raise NotFoundError(f"Plugin with ID {plugin_id} not found")
    
    response = await delete_plugin_from_model(db, db_plugin)
    return response 

async def get_plugins_summary(db: AsyncSession):
    """Get a summary count of plugins by type."""
    stmt = (
        select(Plugin.type, func.count(Plugin.id))
        .group_by(Plugin.type)
    )
    result = await db.execute(stmt)
    type_counts = dict(result.all())
    
    summary = {plugin_type.value: 0 for plugin_type in PluginType}
    summary.update({k.value: v for k, v in type_counts.items()})
    summary['total'] = sum(summary.values())
    
    return summary

async def count_plugins_by_class(db: AsyncSession, plugin_class: PluginClass):
    """Count plugins of a specific class."""
    query = select(func.count(Plugin.id)).where(Plugin.plugin_class == plugin_class)
    result = await db.execute(query)
    count = result.scalar_one()
    return count

async def count_plugins_linked_to_embedding_id(db: AsyncSession, embedding_id: int) -> int:
    """Count plugins linked to a specific embedding ID."""
    query = (select(func.count())
             .select_from(plugin_embeddings)
             .where(plugin_embeddings.c.embedding_id == embedding_id)
             )

    result = await db.execute(query)
    count = result.scalar_one()
    return count

async def count_plugins_linked_to_embedding_class(db: AsyncSession, plugin_class: PluginClass) -> int:
    """Count plugins linked to embeddings of a specific class."""
    embedding_plugin = aliased(EmbeddingPlugin)
    plugin_embedding = aliased(plugin_embeddings)

    query = (
        select(func.count())
        .select_from(plugin_embedding)
        .join(embedding_plugin, embedding_plugin.id == plugin_embedding.c.embedding_id)
        .where(embedding_plugin.plugin_class == plugin_class)
    )
    result = await db.execute(query)
    count = result.scalar_one()
    return count