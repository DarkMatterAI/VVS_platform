from typing import Optional, Dict, Any, Union, List
from collections import defaultdict

from tests.utils.request_data import get_test_embedding, generate_item
from tests.utils.backend_utils import backend_get_plugins_by_filter

from vvs_database.execution.connections import get_connections

from vvs_database import crud 
from vvs_database.execution.ops import (
    ItemOp, 
    DataOp, 
    MapperOp, 
    AssemblyOp, 
    SingleDataOp, 
    DecomposedDataOp,
    MapperDataOp
)

from vvs_database.schemas import (
    Embedding,
    ExecuteParams,
    ExecuteDataParams,
    ExecutePlugin,
    ExecuteDataSource,
    PluginOverrideParams,
    PluginType,
    Query,
    QueryEmbedding,
    AssembledEmbedding,
    InternalItem,
    ItemData 
)

def build_execute_plugin(
    plugin_record: dict,
    *,
    execute_params: Optional[ExecuteParams] = None,
    override_params: Optional[PluginOverrideParams] = None,
    runtime_args: Optional[Dict[str, Any]] = None,
    k: int=5,
    assembly_index: int=0
) -> Union[ExecutePlugin, ExecuteDataSource]:
    """
    Wrap a mock PluginInDB record in the ExecutePlugin*/ExecuteDataSource model
    that ops expect.
    """

    execute_params = execute_params or ExecuteParams()
    override_params = override_params or PluginOverrideParams()

    base_kwargs = dict(
        plugin_id=plugin_record["id"],
        execute_params=execute_params,
        override_params=override_params,
        runtime_args=runtime_args,
        plugin=plugin_record,
    )

    if plugin_record["type"] == PluginType.DATA_SOURCE:
        data_params = ExecuteDataParams(k=k, assembly_index=assembly_index)
        return ExecuteDataSource(data_source_params=data_params, **base_kwargs)

    return ExecutePlugin(**base_kwargs)

def get_test_query(plugin_record):
    embedding = get_test_embedding(plugin_record['id'], plugin_record['vector_length'], plugin_record['name'])
    query = Query(queries=[QueryEmbedding(query_group=0, 
                                           embedding=embedding, 
                                           assembled_embeddings=None)])
    return query 

def get_test_decomposed_query(plugin_records):
    embeddings = []
    for i, plugin_record in enumerate(plugin_records):
        emb = get_test_embedding(plugin_record['id'], 
                                 plugin_record['vector_length'], 
                                 plugin_record['name'])
        embeddings.append(AssembledEmbedding(embedding=emb, assembly_index=i))

    query = Query(queries=[QueryEmbedding(query_group=0,
                                          embedding=None,
                                          assembled_embeddings=embeddings)])
    return query 

async def get_internal_items(db_session, n_items: int):
    items = []
    for i in range(n_items):
        item = await generate_item(db_session)
        item = InternalItem(
            item_data=ItemData(item_id=item.id, external_id=None, item=item.item),
            valid=True,
            score=None,
            embeddings={},
            assembly_data=None,
            query_group=0,
        )
        items.append(item)
    return items 

def assert_has_embedding(items, emb_id):
    for it in items:
        assert emb_id in it.embeddings
        assert it.embeddings[emb_id].embedding, "Empty embedding array"

async def execute_item_op(connections, db_session, score_plugin: dict, embed_plugins: List[dict]):

    score_execute = build_execute_plugin(score_plugin)
    embed_executes = [build_execute_plugin(i) for i in embed_plugins]

    op = ItemOp(score_execute, embedding_configs=embed_executes, 
                connections=connections, log_id='test')
    items = await get_internal_items(db_session, n_items=3)

    result = await op(items)
    for plugin in embed_plugins:
        assert_has_embedding(result, plugin['id'])

    for it in result:
        assert it.score is not None and it.score.plugin_id == score_execute.plugin.id

    return op, result


async def execute_data_op(
    connections,
    db_session,
    data_plugins: Dict[int, dict],     # {assembly_index: plugin_record}
    *,
    n_queries: int = 2,                # how many query embeddings per index
):
    data_cfgs = {}
    request_dict = defaultdict(list)

    # Create ExecuteDataSource configs AND request dict
    for assembly_idx, dsp in data_plugins.items():
        data_cfgs[assembly_idx] = build_execute_plugin(
            dsp, k=5, assembly_index=assembly_idx
        )

        embedding_plugin = await crud.get_plugin(db_session, dsp['embedding_ids'][0])
        for _ in range(n_queries):
            emb = Embedding(**get_test_embedding(embedding_plugin.id, 
                                                 embedding_plugin.vector_length,
                                                 embedding_plugin.name))
            request_dict[assembly_idx].append(emb)

    op = DataOp(data_cfgs, connections, log_id="test")
    response_dict = await op(request_dict)

    # quick sanity checks
    assert response_dict.keys() == request_dict.keys()
    for idx in request_dict:
        assert len(response_dict[idx]) == len(request_dict[idx])
        for resp in response_dict[idx]:
            assert resp.valid, f"invalid response from datasource {idx}"

    return op, request_dict, response_dict


async def execute_mapper_op(
    connections,
    db_session,
    mapper_plugin: dict,
    *,
    n_queries: int = 2,
):
    mapper_cfg = build_execute_plugin(mapper_plugin)
    embedding_plugin = await crud.get_plugin(db_session, mapper_plugin['input_embedding_id'])

    requests = [Embedding(**get_test_embedding(embedding_plugin.id, 
                                               embedding_plugin.vector_length,
                                               embedding_plugin.name))
                        for i in range(n_queries)]
    
    output_embeddings = []
    for output in mapper_plugin['output_order']:
        embedding_plugin = await crud.get_plugin(db_session, output['embedding_id'], response_model=True)
        output_embeddings.append(build_execute_plugin(embedding_plugin.model_dump()))

    op = MapperOp(mapper_cfg, output_embeddings, connections, log_id="test")
    response_dict = await op(requests)

    # checks
    assert set(response_dict.keys()) == {
        oe["index"] for oe in mapper_plugin["output_order"]
    }
    for lst in response_dict.values():
        assert len(lst) == n_queries
        for emb in lst:
            assert emb.embedding, "empty embedding returned"

    return op, requests, response_dict

async def execute_assembly_op(
    connections,
    assembly_plugin: dict,
    request_dict,
    response_dict,
):

    assembly_cfg = build_execute_plugin(assembly_plugin)
    op = AssemblyOp(assembly_cfg, connections, log_id="test")
    items = await op(request_dict, response_dict)

    assert items, "AssemblyOp returned no items"
    for it in items:
        assert it.assembly_data and it.assembly_data.parents
    return op, items


async def execute_single_data_op(connections, db_session, data_plugin: dict):
    data_cfg = build_execute_plugin(data_plugin, k=5, assembly_index=0)

    # build query with the datasource’s input‑embedding plugin
    emb_plg = await crud.get_plugin(db_session, data_plugin["embedding_ids"][0], response_model=True)
    query = get_test_query(emb_plg.model_dump())

    op = SingleDataOp(data_cfg, connections, log_id="test")
    items = await op(query)

    assert items, "SingleDataOp produced no items"
    for it in items:
        assert it.valid
        # update_embedding should echo the original query embedding
        assert it.update_embedding and it.update_embedding.plugin_id == emb_plg.id

    return op, items

async def execute_decomposed_data_op(
    connections,
    db_session,
    data_plugins: dict[int, dict],  # {assembly_index: plugin_record}
    assembly_plugin: dict,
):
    """Run DecomposedDataOp and return items."""
    data_cfgs = {
        idx: build_execute_plugin(plg, k=5, assembly_index=idx)
        for idx, plg in data_plugins.items()
    }
    assembly_cfg = build_execute_plugin(assembly_plugin)

    # build query with parent embeddings
    parent_emb_plugins = [
        await crud.get_plugin(db_session, plg["embedding_ids"][0], response_model=True) 
        for plg in data_plugins.values()
    ]
    query = get_test_decomposed_query([p.model_dump() for p in parent_emb_plugins])

    op = DecomposedDataOp(data_cfgs, assembly_cfg, connections, log_id="test")
    items = await op(query)

    assert items, "DecomposedDataOp produced no items"
    for it in items:
        assert it.assembly_data and len(it.assembly_data.parents) == len(data_plugins)

    return op, items

async def execute_mapper_data_op(
    connections,
    db_session,
    mapper_plugin: dict,
    data_plugins: dict[int, dict],
    assembly_plugin: dict,
):
    mapper_cfg = build_execute_plugin(mapper_plugin)
    input_emb_plugin = await crud.get_plugin(db_session, 
                                             mapper_plugin["input_embedding_id"], 
                                             response_model=True)

    output_cfgs = []
    for oe in mapper_plugin["output_order"]:
        emb_plg = await crud.get_plugin(db_session, 
                                        oe["embedding_id"], 
                                        response_model=True)
        output_cfgs.append(build_execute_plugin(emb_plg.model_dump()))

    data_cfgs = {
        idx: build_execute_plugin(dsp, k=5, assembly_index=idx)
        for idx, dsp in data_plugins.items()
    }
    assembly_cfg = build_execute_plugin(assembly_plugin)
    input_embed_cfg = build_execute_plugin(input_emb_plugin.model_dump())

    # query with the mapper’s *input* embedding
    query = get_test_query(input_emb_plugin.model_dump())

    op = MapperDataOp(
        mapper_cfg,
        input_embed_cfg,
        output_cfgs,
        data_cfgs,
        assembly_cfg,
        connections,
        log_id="test",
    )

    items = await op(query)

    assert items, "MapperDataOp returned no items"
    for it in items:
        # MapperDataOp should later fill in embeddings via ItemOp
        assert it.update_embedding and it.update_embedding.plugin_id == input_emb_plugin.id
        assert it.embeddings  # at least one embedding filled

    return op, items

async def item_op_test_helper(db_session, backend_client, query_str):
    connections = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)
    score_plugin = [i for i in plugins if i['type']=='score'][0]
    embed_plugin = [i for i in plugins if i['type']=='embedding'][0]

    op, results = await execute_item_op(connections, db_session, score_plugin, [embed_plugin])

    await connections.close()

async def data_op_test_helper(db_session, backend_client, query_str):
    connections = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)
    data_sources = [i for i in plugins if i['type'] == 'data_source']
    assert len(data_sources)>0

    data_plugins = {i:d for i,d in enumerate(data_sources)}

    op, request_dict, response_dict = await execute_data_op(
        connections, db_session, data_plugins
    )

    await connections.close()

async def mapper_op_test_helper(db_session, backend_client, query_str):
    connections = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)
    mapper_plugin = [i for i in plugins if i['type']=='mapper'][0]

    op, requests, response_dict = await execute_mapper_op(
        connections, db_session, mapper_plugin
    )

    await connections.close()

async def assembly_op_test_helper(db_session, backend_client, query_str):
    connections = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)

    data_sources = [i for i in plugins if i['type'] == 'data_source']
    assert len(data_sources)>0
    data_sources = [data_sources[0], data_sources[0]] # should be only 1, we need 2

    data_plugins = {i:d for i,d in enumerate(data_sources)}

    op, request_dict, response_dict = await execute_data_op(
        connections, db_session, data_plugins
    )

    assembly_plugin = [i for i in plugins if i['type']=='assembly'][0]

    op, items = await execute_assembly_op(
        connections, assembly_plugin, request_dict, response_dict
    )

    for it in items:
        assert len(it.assembly_data.parents) == 2

    await connections.close()

async def single_data_op_test_helper(db_session, backend_client, query_str):
    conns = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)
    data_plugin = [i for i in plugins if i['type'] == 'data_source'][0]

    op, items = await execute_single_data_op(conns, db_session, data_plugin)

    await conns.close()

async def decomposed_data_op_test_helper(db_session, backend_client, query_str):
    conns = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)

    data_sources = [i for i in plugins if i['type'] == 'data_source']
    assert len(data_sources)>0
    data_plugins = {0: data_sources[0], 1: data_sources[0]}

    assembly_plugin = [i for i in plugins if i['type']=='assembly'][0]

    op, items = await execute_decomposed_data_op(
        conns, db_session, data_plugins, assembly_plugin
    )

    await conns.close()

async def mapper_data_op_test_helper(db_session, backend_client, query_str):
    conns = get_connections(db_session)

    plugins = backend_get_plugins_by_filter(backend_client, query_str)
    mapper_plugin = [i for i in plugins if i['type']=='mapper'][0]
    data_sources = [i for i in plugins if i['type'] == 'data_source']
    needed = {oe["index"] for oe in mapper_plugin["output_order"]}
    data_plugins = {idx: data_sources[0] for idx in needed}
    assembly_plugin = [i for i in plugins if i['type']=='assembly'][0]

    op, items = await execute_mapper_data_op(
        conns, db_session, mapper_plugin, data_plugins, assembly_plugin
    )

    await conns.close()

