from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload, undefer_group
from sqlalchemy import insert, delete, func, and_
from fastapi import HTTPException
from typing import List, Optional, Dict, Any 
from pydantic import ValidationError 
import asyncio

from app import models, schemas, utils 

async def validate_embedding_ids(db: AsyncSession, embedding_ids: List[int]) -> None:
    if (len(embedding_ids) == 0):
        return 
    
    if len(embedding_ids) != len(set(embedding_ids)):
        raise HTTPException(status_code=400, detail="Duplicate embedding IDs are not allowed")

    stmt = select(models.EmbeddingPlugin).filter(models.EmbeddingPlugin.id.in_(embedding_ids))
    result = await db.execute(stmt)
    valid_embeddings = result.scalars().all()

    if len(valid_embeddings) != len(embedding_ids):
        invalid_ids = set(embedding_ids) - set(e.id for e in valid_embeddings)
        raise HTTPException(status_code=400, detail=f"Invalid embedding IDs: {invalid_ids}")

async def get_plugin(db: AsyncSession, plugin_id: int):
    stmt = (
        select(models.Plugin)
        .options(
            selectinload(models.Plugin.embeddings),
            undefer_group('*')  # This will load all column-based attributes
        )
        .filter(models.Plugin.id == plugin_id)
    )
    result = await db.execute(stmt)
    plugin = result.scalars().first()
    
    if plugin:
        # Force loading of all attributes
        await db.refresh(plugin)
    
    return plugin

def build_filters(model, filter_params):
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
    stmt = (
        select(models.Plugin)
        .options(
            selectinload(models.Plugin.embeddings),
            undefer_group('*')
        )
    )

    if filter_params:
        filters = build_filters(models.Plugin, filter_params)
        stmt = stmt.filter(and_(*filters))

    stmt = stmt.offset(skip).limit(limit)

    result = await db.execute(stmt)
    plugins = result.scalars().all()
    
    for plugin in plugins:
        await db.refresh(plugin)
    
    return plugins


async def create_plugin(db: AsyncSession, plugin: schemas.PluginCreate):
    plugin_model = utils.get_plugin_data_model(plugin.type)
    plugin_data = plugin.model_dump(exclude={'embedding_ids', 'input_embedding_id', 'output_embedding_ids'})

    if isinstance(plugin, schemas.MapperPluginCreate):
        await validate_embedding_ids(db, [plugin.input_embedding_id] + plugin.output_embedding_ids)
        plugin_data['input_embedding_id'] = plugin.input_embedding_id
        embedding_ids = plugin.output_embedding_ids
    elif hasattr(plugin, 'embedding_ids') and (plugin.embedding_ids is not None):
        await validate_embedding_ids(db, plugin.embedding_ids)
        embedding_ids = plugin.embedding_ids
    else:
        embedding_ids = []

    db_plugin = plugin_model(**plugin_data)
    db.add(db_plugin)
    await db.flush()

    if embedding_ids:
        await db.execute(
            insert(models.plugin_embeddings).values([
                {"plugin_id": db_plugin.id, "embedding_id": embedding_id}
                for embedding_id in embedding_ids
            ])
        )

    await db.commit()
    stmt = select(plugin_model).options(selectinload(plugin_model.embeddings)).filter_by(id=db_plugin.id)
    result = await db.execute(stmt)
    db_plugin = result.scalar_one()
    return db_plugin

def embedding_update(db_plugin, update_data):
    if 'vector_length' in update_data:
        db_plugin.vector_length = update_data['vector_length']
    if 'distance_metric' in update_data:
        db_plugin.distance_metric = update_data['distance_metric']


def assembly_update(db_plugin, update_data):
    if 'num_parents' in update_data:
        db_plugin.num_parents = update_data['num_parents']

async def update_linked_embeddings(db_plugin, update_data, db, key='embedding_ids'):
    if key in update_data:
        await validate_embedding_ids(db, update_data[key])
        await db.execute(
            delete(models.plugin_embeddings).where(models.plugin_embeddings.c.plugin_id == db_plugin.id)
        )
        
        if update_data[key]:
            await db.execute(
                insert(models.plugin_embeddings).values([
                    {"plugin_id": db_plugin.id, "embedding_id": embedding_id}
                    for embedding_id in update_data[key]
                ])
            )

async def mapper_update(db_plugin, update_data, db):
    if 'output_embedding_ids' in update_data:
        await update_linked_embeddings(db_plugin, update_data, db, key='output_embedding_ids')

    if 'input_embedding_id' in update_data:
        await validate_embedding_ids(db, [update_data['input_embedding_id']])
        db_plugin.input_embedding_id = update_data['input_embedding_id']

async def update_plugin(db: AsyncSession, plugin_id: int, plugin: schemas.PluginUpdate):
    db_plugin = await get_plugin(db, plugin_id)

    if not db_plugin:
        return None
    
    update_data = plugin.model_dump(exclude_unset=True)

    try:
        utils.validate_updates(db_plugin, update_data)
    except ValidationError as e:
        raise HTTPException(
            status_code=422,
            detail=str(e)
        )
    
    general_fields = ['name', 'timeout', 'max_concurrency', 'max_retries', 
                      'endpoint_url', 'group_key', 'config', 'plugin_metadata']
    for field in general_fields:
        if field in update_data:
            setattr(db_plugin, field, update_data[field])

    # Update specific fields based on plugin type
    if isinstance(db_plugin, models.EmbeddingPlugin):
        embedding_update(db_plugin, update_data)

    elif isinstance(db_plugin, models.AssemblyPlugin):
        assembly_update(db_plugin, update_data)

    elif isinstance(db_plugin, (models.DataSourcePlugin, models.FilterPlugin, models.ScorePlugin)):
        await update_linked_embeddings(db_plugin, update_data, db)

    elif isinstance(db_plugin, models.MapperPlugin):
        await mapper_update(db_plugin, update_data, db)


    await db.commit()
    await db.refresh(db_plugin)

    # Grab database entry again to populate association data - required to avoid implicit i/o error
    db_plugin = await get_plugin(db, db_plugin.id)
    return db_plugin

async def delete_plugin(db: AsyncSession, plugin_id: int):
    db_plugin = await get_plugin(db, plugin_id)
    if not db_plugin:
        return None

    # Check if embedding plugin is linked to other records
    if isinstance(db_plugin, models.EmbeddingPlugin):
        linked_plugins = await db.execute(
            select(models.Plugin)
            .join(models.plugin_embeddings)
            .filter(models.plugin_embeddings.c.embedding_id == plugin_id)
        )
        linked_plugins = linked_plugins.scalars().all()

        mapper_plugins = await db.execute(
            select(models.MapperPlugin)
            .filter(models.MapperPlugin.input_embedding_id == plugin_id)
        )

        linked_plugins += mapper_plugins.scalars().all()

        if linked_plugins:
            linked_plugin_names = [p.name for p in linked_plugins]
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete this embedding plugin. It is linked to the following plugins: {', '.join(linked_plugin_names)}"
            )

    await db.delete(db_plugin)
    await db.commit()
    return db_plugin

async def get_plugins_summary(db: AsyncSession):
    stmt = (
        select(models.Plugin.type, func.count(models.Plugin.id))
        .group_by(models.Plugin.type)
    )
    result = await db.execute(stmt)
    type_counts = dict(result.all())
    
    summary = {plugin_type.value: 0 for plugin_type in schemas.PluginType}
    summary.update({k.value: v for k, v in type_counts.items()})
    summary['total'] = sum(summary.values())
    
    return summary


async def execute_plugin(db_plugin, execute_request):

    try:
        print(f'validating plugin')
        utils.validate_execute_request(db_plugin, execute_request)
        print('validation successful')
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
    execution_type = db_plugin.execution_type.lower()
    if execution_type == 'api':
        response = await utils.execute_api_plugin(db_plugin, execute_request)
    elif execution_type == 'queue':
        response = utils.execute_queue_plugin(db_plugin, execute_request)
        await asyncio.sleep(0)
    elif execution_type == 'internal_tei':
        response = await utils.execute_tei_plugin(db_plugin, execute_request) 
    else:
        raise HTTPException(status_code=400, detail=f"Execute plugin of type {db_plugin['type']} not supported")
    
    return response 