import asyncio
from itertools import islice

import pytest
from sqlalchemy import select, func

from tests.utils.backend_utils import backend_get_plugins_by_filter

from vvs_database.execution.connections import get_connections
from vvs_database import crud 
from vvs_database.utils import object_as_dict
from vvs_database.job_runner.hc_runner.hc_runner import HCRunner
from vvs_database.crud.hc_crud import export_hc_job_hierarchy, create_hc_job, fetch_hc_job_results
from vvs_database.models import HCResult, HCJob, HCInputJob
from vvs_database.schemas import (
    JobStatus,
    DistanceMetric,
    TERMINAL_STATUSES,
    MapperPluginCreate,
    PluginType,
)
from vvs_database.schemas.hc_schemas import (
    HCConfigCreate,
    HCJobCreate,
    HCJobParams,
    HCUpdateParams,
    UpdateType,
    HCAssembledConfigCreate,
    LearningRate,
    HCAssembledJobCreate,
    HCAssembledUpdateParams,
    HCMapperConfigCreate,
    HCMapperJobCreate,
    HCInputItem,
    HCAssembedInputItem
)

from vvs_database.schemas.internal_schemas import (
    ExecutePluginCreate,
    ExecuteDataSourceCreate,
    ExecuteDataParams,
    ExecuteParams
)


# ----------------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------------

def _pick_first(plugins, typ):
    try:
        return next(p for p in plugins if p["type"] == typ)
    except StopIteration:  # pragma: no cover
        pytest.skip(f"No plugin of type {typ!r} registered in backend")

def _pick_n(plugins, typ, n):
    sel = [p for p in plugins if p["type"] == typ][:n]
    if len(sel) < n:
        pytest.skip(f"Need {n} plugins of type {typ}")
    return sel

def _hc_base_parts(*, embeddings, datasource_plugins, filter_plugins, score_plugin, 
                   mapper_plugin=None, assembly_plugin=None):
    """Return kwargs for the HC*Config* schemas."""
    filter_configs = [ExecutePluginCreate(plugin_id=p['id']) for p in filter_plugins]
    score_config = ExecutePluginCreate(plugin_id=score_plugin['id'], execute_params=ExecuteParams())

    data_configs = [
        ExecuteDataSourceCreate(
            plugin_id=p['id'],
            data_source_params=ExecuteDataParams(k=3, assembly_index=i),
        )
        for i, p in enumerate(datasource_plugins)
    ]
    if embeddings is None:
        embeddings = []

    embedding_configs = [ExecutePluginCreate(plugin_id=e['id']) for e in embeddings]

    return dict(
        filter_configs=filter_configs,
        score_config=score_config,
        embedding_configs=embedding_configs,
        data_configs=data_configs,
        assembly_config=ExecutePluginCreate(plugin_id=assembly_plugin['id']) if assembly_plugin else None,
        mapper_config=ExecutePluginCreate(plugin_id=mapper_plugin['id']) if mapper_plugin else None,
    )

def _mk_job_inputs(single=True, max_iterations=5):
    if single:
        return [HCInputItem(item={"external_id": "ZINC1", "item": "C1=CC=CC=C1"}, max_iterations=max_iterations)]
    # assembled – two sub‑items, indices 0 and 1
    sub_items = [
        {"external_id": "EN1", "item": "NCC", "assembly_index": 0},
        {"external_id": "EN2", "item": "CCC", "assembly_index": 1},
    ]
    return [HCAssembedInputItem(items=sub_items, max_iterations=max_iterations)]


# ----------------------------------------------------------------------------
# 1. Standard variant (1 data source)
# ----------------------------------------------------------------------------

async def _create_standard_hc_job(db_session, backend_client):
    """
    Return (parent_job, [input_job], connections).
    Uses whatever mock plugins are currently registered in the backend.
    """
    # ---- 1. discover plugins ------------------------------------------------
    plugins = backend_get_plugins_by_filter(backend_client, "mock_%")
    datasource = _pick_first(plugins, "data_source")
    embed_plg  = await crud.get_plugin(db_session, datasource["embedding_ids"][0], response_model=True)
    filter_plg = _pick_first(plugins, "filter")
    score_plg  = _pick_first(plugins, "score")

    # ---- 2. build HCConfigCreate payload -----------------------------------
    create_args = _hc_base_parts(
        embeddings=[embed_plg.model_dump()],
        datasource_plugins=[datasource],
        filter_plugins=[filter_plg],
        score_plugin=score_plg,
    )
    data_cfg = create_args.pop("data_configs")[0]

    cfg = HCConfigCreate(**create_args, data_config=data_cfg)

    create_obj = HCJobCreate(
        job_params=HCJobParams(auto_execute=False),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[1.0, 2.0, 3.0],
        ),
        job_inputs=_mk_job_inputs(single=True, max_iterations=3),
    )

    *_ , parent_job, input_jobs = await create_hc_job(db_session, create_obj)
    return parent_job, input_jobs

# ----------------------------------------------------------------------------
# 2. Assembled variant (Assembly + 2 data sources)
# ----------------------------------------------------------------------------
async def _create_assembled_job(db, backend_client):
    plugins = backend_get_plugins_by_filter(backend_client, "mock_%")

    assembly_plg = _pick_first(plugins, "assembly")
    datasources  = _pick_n(plugins, "data_source", 1)
    filter_plg   = _pick_first(plugins, "filter")
    score_plg    = _pick_first(plugins, "score")

    create_args = _hc_base_parts(
        embeddings=[],
        # embeddings=[emb0.model_dump(), emb1.model_dump()],
        datasource_plugins=datasources,
        filter_plugins=[filter_plg],
        score_plugin=score_plg,
        assembly_plugin=assembly_plg,
    )

    cfg = HCAssembledConfigCreate(
        **create_args,
        update_params=None,   # placeholder, filled below
    )

    lr_cfg = [
        LearningRate(learning_rate=[1.0, 2.0], assembly_index=0),
        LearningRate(learning_rate=[1.5, 2.5], assembly_index=1),
    ]

    create_obj = HCAssembledJobCreate(
        job_params=HCJobParams(auto_execute=False),
        plugin_config=cfg,
        update_params=HCAssembledUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=lr_cfg,
        ),
        job_inputs=_mk_job_inputs(single=False, max_iterations=3),
    )
    *_ , parent_job, inputs = await create_hc_job(db, create_obj)
    return parent_job, inputs


# ----------------------------------------------------------------------------
# 2. Mapper variant (Mapper + Assembly + Data Sources)
# ----------------------------------------------------------------------------
async def _create_mapper_job(db, backend_client, plugin_cleanup):
    plugins = backend_get_plugins_by_filter(backend_client, "mock_%")

    mapper_plg   = _pick_first(plugins, "mapper")
    assembly_plg = _pick_first(plugins, "assembly")
    score_plg    = _pick_first(plugins, "score")
    filter_plg   = _pick_first(plugins, "filter")

    # map outputs → datasources (one ds reused for each index if necessary)
    out_idxs = {o["index"] for o in mapper_plg["output_order"]}
    datasource = _pick_first(plugins, "data_source")
    datasources = [datasource for _ in out_idxs]
    print(datasource)
    output_order = [{'index':i, 'embedding_id':datasources[i]['embedding_ids'][0]}
                    for i in range(len(datasources))]

    mapper_create = MapperPluginCreate(name=mapper_plg['name']+'_tmp',
                                       type=PluginType.MAPPER,
                                       plugin_class=mapper_plg['plugin_class'],
                                       execution_type=mapper_plg['execution_type'],
                                       timeout=mapper_plg['timeout'],
                                       endpoint_url=mapper_plg['endpoint_url'],
                                       group_key=mapper_plg['group_key'],
                                       input_embedding_id=mapper_plg['input_embedding_id'],
                                       output_order=output_order)
    mapper_plg = await crud.create_plugin(db, mapper_create, response_model=True)
    mapper_plg = mapper_plg.model_dump()
    plugin_cleanup(mapper_plg)

    create_args = _hc_base_parts(
        embeddings=None,
        datasource_plugins=datasources,
        filter_plugins=[filter_plg],
        score_plugin=score_plg,
        mapper_plugin=mapper_plg,
        assembly_plugin=assembly_plg,
    )

    cfg = HCMapperConfigCreate(**create_args)

    create_obj = HCMapperJobCreate(
        job_params=HCJobParams(auto_execute=False),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[0.8, 0.4, 0.2],
        ),
        job_inputs=_mk_job_inputs(single=True, max_iterations=3),
    )
    *_ , parent_job, inputs = await create_hc_job(db, create_obj)
    return parent_job, inputs


# ----------------------------------------------------------------------------
# Generic runner‑driver used by both tests
# ----------------------------------------------------------------------------
async def _run_and_assert(db, parent_job, input_job):
    conns = get_connections(db)
    runner = HCRunner(job_id=input_job.id)

    await runner.load_job(db)
    runner.load_ops(conns)
    await runner.init_job(conns)
    await runner.init_first_iteration(db)

    for _ in range(15):          # safety cap: 15 iterations max
        nxt = await runner(conns)
        if nxt is None:
            break
    else:  # pragma: no cover
        pytest.fail("Runner did not converge within 15 iterations")

    await conns.close()

    await db.refresh(parent_job); await db.refresh(input_job)
    assert parent_job.status in TERMINAL_STATUSES
    assert input_job.status  in TERMINAL_STATUSES

    nres = await db.scalar(select(func.count()).select_from(HCResult).where(HCResult.job_id==parent_job.id))
    assert nres > 0

    export_flat = await fetch_hc_job_results(db, parent_job.id, order_by="score", limit=nres+10)
    assert export_flat and export_flat[0]["score"] is not None

    export_nested = await export_hc_job_hierarchy(db, parent_job.id)
    top = export_nested[0]["iterations"][0]["results"][0]
    assert top["score"] is not None


# ----------------------------------------------------------------------------
# Test entry‑points
# ----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hc_runner_standard_end_to_end(
    db_session, backend_client, plugin_cleanup, job_cleanup,
):
    parent_job, [input_job] = await _create_standard_hc_job(db_session, backend_client)
    job_cleanup(object_as_dict(parent_job))

    await _run_and_assert(db_session, parent_job, input_job)

@pytest.mark.asyncio
async def test_runner_assembled(
    db_session, backend_client, plugin_cleanup, job_cleanup
):
    parent_job, [input_job] = await _create_assembled_job(db_session, backend_client)
    job_cleanup(object_as_dict(parent_job))
    await _run_and_assert(db_session, parent_job, input_job)

@pytest.mark.asyncio
async def test_runner_mapper(
    db_session, backend_client, plugin_cleanup, job_cleanup
):
    parent, [input_job] = await _create_mapper_job(db_session, backend_client, plugin_cleanup)
    job_cleanup(object_as_dict(parent))
    await _run_and_assert(db_session, parent, input_job)

