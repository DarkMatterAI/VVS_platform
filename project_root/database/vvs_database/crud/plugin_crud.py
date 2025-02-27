from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload, undefer_group, aliased
from sqlalchemy import insert, delete, func, and_
from typing import List, Optional, Dict, Any, Tuple, Union

from vvs_database import utils, schemas  
from vvs_database.exceptions import ValidationError, NotFoundError, ReferenceError
from vvs_database.models import (
    Plugin, 
    EmbeddingPlugin, 
    DataSourcePlugin, 
    FilterPlugin, 
    ScorePlugin,
    MapperPlugin, 
    AssemblyPlugin, 
    plugin_embeddings
)
# from vvs_database.schemas import (
#     PluginType, 
#     PluginExecutionType,
#     PluginClass, 
#     PluginCreate,
#     MapperPluginCreate,
#     PluginUpdate,
#     ExecuteRequestUnion,
#     BatchExecuteRequestUnion
# )

from vvs_database.crud.item_checkin import result_checkin, item_checkin, assembly_checkin

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

async def get_plugin(db: AsyncSession, plugin_id: int, with_error: bool=True, response_model: bool=False):
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

        if isinstance(plugin, (DataSourcePlugin, FilterPlugin, ScorePlugin, MapperPlugin)):
            await db.refresh(plugin, ["embeddings"])

    if with_error and (plugin is None):
        raise NotFoundError(f"Plugin with ID {plugin_id} not found")
    
    if response_model:
        plugin = utils.get_plugin_response_model(plugin)
    
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
    limit: int = 100,
    response_model: bool = False
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

    if response_model:
        plugins = [utils.get_plugin_response_model(i) for i in plugins]
    
    return plugins

def validate_output_order(output_order):
    """Validate that output order has unique indices."""
    ids = [i['index'] for i in output_order]
    if len(set(ids)) != len(ids):
        raise ValidationError(f"Duplicate index values in output order {ids}")

async def create_plugin_db(
    db: AsyncSession, 
    plugin_type: schemas.PluginType,
    plugin_data: dict,
    embedding_ids: List[int] = None
):
    """Create a new plugin with the given data."""
    plugin_model = utils.get_plugin_data_model(plugin_type)
    
    # Handle mapper plugin special case
    if plugin_type == schemas.PluginType.MAPPER:
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

    await db.flush()
    
    # await db.commit()
    
    # Reload with relationships
    stmt = select(plugin_model).options(selectinload(plugin_model.embeddings)).filter_by(id=db_plugin.id)
    result = await db.execute(stmt)
    db_plugin = result.scalar_one()
    await db.commit()
    return db_plugin

async def create_plugin(db: AsyncSession, plugin: schemas.PluginCreate, response_model: bool=False):
    plugin_data = plugin.model_dump(exclude={'embedding_ids', 'input_embedding_id'})
        
    embedding_ids = []
    if isinstance(plugin, schemas.MapperPluginCreate):
        embedding_order = [i.model_dump() for i in plugin.output_order]
        plugin_data['output_order'] = embedding_order
        plugin_data['input_embedding_id'] = plugin.input_embedding_id
    elif hasattr(plugin, 'embedding_ids') and (plugin.embedding_ids is not None):
        embedding_ids = plugin.embedding_ids
        
    db_plugin = await create_plugin_db(
        db=db,
        plugin_type=plugin.type,
        plugin_data=plugin_data,
        embedding_ids=embedding_ids
    )
    if response_model:
        db_plugin = utils.get_plugin_response_model(db_plugin)

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

async def update_plugin_db(db: AsyncSession, plugin_id: int, update_data: dict):
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

async def update_plugin(db: AsyncSession, 
                        plugin_id: int, 
                        plugin: schemas.PluginUpdate, 
                        response_model: bool=False):
    update_data = plugin.model_dump(exclude_unset=True)
    
    db_plugin = await get_plugin(db, plugin_id)

    utils.validate_updates(db_plugin, update_data)
    db_plugin = await update_plugin_db(db, plugin_id, update_data)

    if response_model:
        db_plugin = utils.get_plugin_response_model(db_plugin)
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
    
    summary = {plugin_type.value: 0 for plugin_type in schemas.PluginType}
    summary.update({k.value: v for k, v in type_counts.items()})
    summary['total'] = sum(summary.values())
    
    return summary

async def count_plugins_by_class(db: AsyncSession, plugin_class: schemas.PluginClass):
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

async def count_plugins_linked_to_embedding_class(db: AsyncSession, plugin_class: schemas.PluginClass) -> int:
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

async def execute_plugin_db(db_plugin: Plugin, 
                            execute_request: Union[schemas.ExecuteRequestUnion, 
                                                   schemas.BatchExecuteRequestUnion]):
    execution_type = db_plugin.execution_type.lower()
    if execution_type not in [i for i in schemas.PluginExecutionType]:
        raise ValidationError(f"Execution type {execution_type} not supported")
    
    plugin_type = db_plugin.type.lower()
    if plugin_type not in [i for i in schemas.PluginType]:
        raise ValidationError(f"Plugin type {plugin_type} execution not supported")

    if type(execute_request) == list:
        utils.validate_execute_request(db_plugin, execute_request)
        execution_function = utils.batch_execute_plugin_map.get(execution_type, None)
    else:
        utils.validate_execute_request(db_plugin, [execute_request])
        execution_function = utils.execute_plugin_map.get(execution_type, None)

    response = await execution_function(db_plugin, execute_request)
    return response 

# async def execute_plugin(db: AsyncSession, plugin_id: int, 
#                          execute_request: Union[ExecuteRequestUnion, BatchExecuteRequestUnion]):
#     db_plugin = await get_plugin(db, plugin_id)
#     response = await execute_plugin_db(db_plugin, execute_request)
#     return response 

async def execute_plugin(db: AsyncSession, plugin_id: int, 
                         execute_request: Union[schemas.ExecuteRequestUnion, 
                                                schemas.BatchExecuteRequestUnion],
                         checkin_result=False):
    """
    Execute a plugin and optionally check in the results to the database.
    
    Args:
        db: Database session
        plugin_id: ID of the plugin to execute
        execute_request: Request data for the plugin
        checkin_result: Whether to check in the results to the database
        
    Returns:
        The plugin execution response
    """
    db_plugin = await get_plugin(db, plugin_id)
    response = await execute_plugin_db(db_plugin, execute_request)

    response_model = utils.plugin_type_map[db_plugin.type]['execute_response_model']
    
    if checkin_result:
        # Check if request is a batch or single request
        is_batch = isinstance(execute_request, list)
        requests = execute_request if is_batch else [execute_request]
        responses = response if is_batch else [response]
        responses = [response_model(**i) for i in responses]
        # print(type(requests[0]), type(responses[0]))
        
        # Process based on plugin type
        if db_plugin.type == schemas.PluginType.EMBEDDING:
            await handle_result_checkin(db, requests, responses, plugin_id, include_embedding=True)
        elif db_plugin.type == schemas.PluginType.FILTER or db_plugin.type == schemas.PluginType.SCORE:
            await handle_result_checkin(db, requests, responses, plugin_id)
        elif db_plugin.type == schemas.PluginType.DATA_SOURCE:
            await handle_item_checkin(db, responses, plugin_id)
        elif db_plugin.type == schemas.PluginType.ASSEMBLY:
            await handle_assembly_checkin(db, requests, responses, plugin_id)
        # MAPPER type has no check-in
    
    return response

async def handle_result_checkin(db: AsyncSession, requests, responses, plugin_id, include_embedding=False):
    """Handle check-in for embedding, filter, and score plugins using result_checkin."""
    new_results = []
    
    for req, resp in zip(requests, responses):
        result_data = {
            "item_id": req.item_data.item_id,
            "valid": resp.valid
        }
        
        # Add score if available (for score plugins)
        if hasattr(resp, "score"):
            result_data["score"] = resp.score
            
        # Add embedding if requested and available (for embedding plugins)
        if include_embedding and hasattr(resp, "embedding") and resp.embedding:
            result_data["embedding"] = resp.embedding
            
        new_results.append(schemas.NewResult(**result_data))
        
    if new_results:
        await result_checkin(db, new_results, plugin_id)

async def handle_item_checkin(db: AsyncSession, responses, plugin_id):
    """Handle check-in for data source plugins using item_checkin."""
    new_items = []
    
    for resp in responses:
        if resp.valid and resp.result:
            for item in resp.result:
                new_items.append(schemas.NewItem(item=item.item, 
                                                 external_id=item.external_id))
                
    if new_items:
        await item_checkin(db, new_items, plugin_id)

async def handle_assembly_checkin(db: AsyncSession, requests, responses, plugin_id):
    """Handle check-in for assembly plugins using assembly_checkin."""
    new_assemblies = []
    
    for req, resp in zip(requests, responses):
        if resp.valid and resp.result:
            for result in resp.result:
                # Get parent components from the request
                components = [
                    {"item_id": parent.item_id, "assembly_index": parent.assembly_index}
                    for parent in req.parents
                ]

                new_assemblies.append(schemas.NewAssembly(item=result.item,
                                                          external_id=result.external_id,
                                                          components=components))
                
    if new_assemblies:
        await assembly_checkin(db, new_assemblies, plugin_id)