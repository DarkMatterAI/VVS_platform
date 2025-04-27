import pytest
from sqlalchemy import select

# ── schema helpers ──────────────────────────────────────────────────────────
from vvs_database.schemas.hc_schemas import (
    HCJobParams,
    HCJobCreate,
    HCUpdateParams,
    HCAssembledUpdateParams,
    HCInputItem,
    HCAssembedInputItem,
    HCInferenceParams,
    UpdateType,
    LearningRate,
    HCConfigCreate
)

from vvs_database.schemas import DistanceMetric
from vvs_database.schemas.internal_schemas import ExecutePluginCreate, ExecuteDataSourceCreate, ExecuteDataParams

from vvs_database.models.job_models.hc_models import HCInputJob
from vvs_database.models import JobPlugin

from vvs_database.crud.hc_crud import create_hc_job

from tests.hc_crud.conftest import _hc_base_parts


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _mk_job_inputs_with_overrides(
    *, single: bool = True,
    inf_limit: int = 42, time_limit: int = 123,
    upd_lr: list[float] | None = None,
):
    inf = HCInferenceParams(inference_limit=inf_limit, time_limit=time_limit)

    upd = None
    if upd_lr is not None:
        upd = HCUpdateParams(
            update_type=UpdateType.GROUP_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=upd_lr,
        )

    if single:
        return [
            HCInputItem(
                item={"external_id": "Z1", "item": "C1=CC=CC=C1"},
                max_iterations=3,
                inference_params=inf,
                update_params=upd,
            )
        ]

    # assembled variant (indices 0,1)
    items = [
        {"external_id": "EN1", "item": "NCC", "assembly_index": 0},
        {"external_id": "EN2", "item": "CCC", "assembly_index": 1},
    ]
    return [
        HCAssembedInputItem(
            max_iterations=3,
            inference_params=inf,
            update_params=upd,        # same override applies to both parents
            items=items,
        )
    ]


# ---------------------------------------------------------------------------
# 1. inference_params → columns propagate
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_input_inference_params_propagate(
    db_session,
    create_test_datasource_plugin,
    create_test_embedding,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    # ---- minimal standard config ------------------------------------------
    datasource, emb = await create_test_datasource_plugin()
    filter_p        = await create_test_filter_plugin()
    score_p         = await create_test_score_plugin()

    cfg_kwargs = _hc_base_parts(
        embeddings=[emb],
        datasource_plugins=[datasource],
        filter_plugins=[filter_p],
        score_plugin=score_p,
    )
    data_cfg = cfg_kwargs.pop("data_configs")[0]

    hc_cfg = HCJobCreate(
        job_params=HCJobParams(),
        plugin_config=HCConfigCreate(**cfg_kwargs, data_config=data_cfg),
        update_params=HCUpdateParams(               # parent-level defaults
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[0.1, 0.2, 0.3],
        ),
        job_inputs=_mk_job_inputs_with_overrides(
            inf_limit=77, time_limit=88, upd_lr=None, single=True
        ),
    )

    _, _, parent_job, [input_job] = await create_hc_job(db_session, hc_cfg)

    # ---- assertions --------------------------------------------------------
    await db_session.refresh(input_job)
    assert input_job.inference_limit == 77
    assert input_job.time_limit == 88
    # parent keeps None (came from HCJobParams default)
    await db_session.refresh(parent_job)
    assert parent_job.inference_limit is None and parent_job.time_limit is None
    await db_session.commit()


# ---------------------------------------------------------------------------
# 2. per-input update_params override stored in job_json
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_input_update_params_override(
    db_session,
    create_test_datasource_plugin,
    create_test_embedding,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    datasource, emb = await create_test_datasource_plugin()
    filter_p        = await create_test_filter_plugin()
    score_p         = await create_test_score_plugin()

    cfg_kwargs = _hc_base_parts(
        embeddings=[emb],
        datasource_plugins=[datasource],
        filter_plugins=[filter_p],
        score_plugin=score_p,
    )
    data_cfg = cfg_kwargs.pop("data_configs")[0]

    # parent-level params (learning_rate=[9,9,9])
    parent_update = HCUpdateParams(
        update_type=UpdateType.GLOBAL_UPDATE,
        distance_metric=DistanceMetric.Cosine,
        learning_rate=[9.0, 9.0, 9.0],
    )

    # child override (learning_rate=[1,2,3])
    child_lr = [1.0, 2.0, 3.0]

    hc_cfg = HCJobCreate(
        job_params=HCJobParams(),
        plugin_config=HCConfigCreate(**cfg_kwargs, data_config=data_cfg),
        update_params=parent_update,
        job_inputs=_mk_job_inputs_with_overrides(
            inf_limit=5, time_limit=10, upd_lr=child_lr, single=True
        ),
    )

    _, _, parent_job, [input_job] = await create_hc_job(db_session, hc_cfg)

    # ---- assertions --------------------------------------------------------
    # parent keeps its original learning rate
    assert parent_job.job_json["update_params"]["learning_rate"] == [9.0, 9.0, 9.0]

    # child job_json now contains assembled override   (index 0 wrapper)
    lr_cfg = input_job.job_json["update_params"]["learning_rate"]
    assert isinstance(lr_cfg, list) and lr_cfg[0]["learning_rate"] == child_lr

    stmt = select(JobPlugin.plugin_id).where(JobPlugin.job_id == parent_job.id)
    plugin_ids = {pid for (pid,) in (await db_session.execute(stmt)).all()}
    expected = {datasource.id, emb.id, filter_p.id, score_p.id}
    assert expected.issubset(plugin_ids)
    await db_session.commit()
