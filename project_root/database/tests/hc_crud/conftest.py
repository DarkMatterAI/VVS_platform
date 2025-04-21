import pytest
import pytest_asyncio

from vvs_database.crud.hc_crud import create_hc_job
from vvs_database.schemas import (
    DistanceMetric,
    ExecuteParams,
)
from vvs_database.schemas.hc_schemas import HCConfigCreate

from vvs_database.schemas.hc_schemas import (
    HCJobParams,
    HCConfigCreate,
    HCUpdateParams,
    HCJobCreate,
    HCInputItem,
    HCAssembedInputItem,
    UpdateType,
)

from vvs_database.schemas.internal_schemas import (
    ExecutePluginCreate,
    ExecuteDataSourceCreate,
    ExecuteDataParams,
    ExecuteParams
)

def _hc_base_parts(*, embeddings, datasource_plugins, filter_plugins, score_plugin, 
                   mapper_plugin=None, assembly_plugin=None):
    """Return kwargs for the HC*Config* schemas."""
    filter_configs = [ExecutePluginCreate(plugin_id=p.id) for p in filter_plugins]
    score_config = ExecutePluginCreate(plugin_id=score_plugin.id, execute_params=ExecuteParams())

    data_configs = [
        ExecuteDataSourceCreate(
            plugin_id=p.id,
            data_source_params=ExecuteDataParams(k=10, assembly_index=i),
        )
        for i, p in enumerate(datasource_plugins)
    ]
    if embeddings is None:
        embeddings = []

    embedding_configs = [ExecutePluginCreate(plugin_id=e.id) for e in embeddings]

    return dict(
        filter_configs=filter_configs,
        score_config=score_config,
        embedding_configs=embedding_configs,
        data_configs=data_configs,
        assembly_config=ExecutePluginCreate(plugin_id=assembly_plugin.id) if assembly_plugin else None,
        mapper_config=ExecutePluginCreate(plugin_id=mapper_plugin.id) if mapper_plugin else None,
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

@pytest_asyncio.fixture(scope="function")
async def create_hc_job_standard(
    db_session,
    create_test_datasource_plugin,
    create_test_embedding,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    """
    Async factory that builds the minimal 'standard' HCJob variant
    (no mapper, no assembly) and returns the same 4-tuple as
    create_hc_job():  (search_cfg, item_dict, parent_job, input_jobs).

        search_cfg   :: HCSearchConfigs
        item_dict    :: dict[ input-idx -> {job_inputs …} ]
        parent_job   :: HCJob  (polymorphic parent)
        input_jobs   :: List[HCInputJob]   (one per element of job_inputs)
    """
    async def _create_hc_job_standard(max_iterations=5):
        datasource, emb = await create_test_datasource_plugin()
        filter_p        = await create_test_filter_plugin()
        score_p         = await create_test_score_plugin()

        create_args = _hc_base_parts(
            embeddings=[emb],
            datasource_plugins=[datasource],
            filter_plugins=[filter_p],
            score_plugin=score_p,
        )
        data_cfg = create_args.pop("data_configs")[0]

        cfg = HCConfigCreate(**create_args, data_config=data_cfg)

        create_obj = HCJobCreate(
            job_params=HCJobParams(),
            plugin_config=cfg,
            update_params=HCUpdateParams(
                update_type=UpdateType.GLOBAL_UPDATE,
                distance_metric=DistanceMetric.Cosine,
                learning_rate=[1.0, 2.0, 3.0],
            ),
            job_inputs=_mk_job_inputs(single=True, max_iterations=max_iterations),
        )

        return await create_hc_job(db_session, create_obj)

    return _create_hc_job_standard

