import pytest
import pytest_asyncio
from sqlalchemy import select

from vvs_database.exceptions import ValidationError as CrudValidationError
from pydantic import ValidationError as PydanticValidationError

from vvs_database import crud
from vvs_database.crud.hc_crud import create_hc_job
from vvs_database.models import HCInputItems, Item, JobPlugin
from vvs_database.schemas import (
    DistanceMetric,
    ExecuteParams,
)
from vvs_database.schemas.hc_schemas import (
    HCConfigCreate,
    HCMapperConfigCreate,
    HCAssembledConfigCreate,
)

from vvs_database.schemas.hc_schemas import (
    HCJobParams,
    HCConfigCreate,
    HCMapperJobCreate,
    HCAssembledJobCreate,
    HCUpdateParams,
    HCAssembledUpdateParams,
    HCInputItem,
    HCJobCreate,
    HCAssembedInputItem,
    UpdateType,
    LearningRate
)

from vvs_database.schemas.internal_schemas import (
    ExecutePluginCreate,
    ExecuteDataSourceCreate,
    PluginOverrideParams,
    ExecuteDataParams,
    ExecuteParams
)

from vvs_database.schemas.enums import JobType

from tests.hc_crud.conftest import _hc_base_parts, _mk_job_inputs

async def _assert_parent_and_input_rows(db_session, parent_job, input_jobs):
    assert parent_job.job_type == JobType.HILL_CLIMB_JOB
    assert len(input_jobs) == 1
    stmt = select(HCInputItems, Item).join(Item).where(HCInputItems.job_id == input_jobs[0].id)
    row = (await db_session.execute(stmt)).one()
    hc_item, item = row
    assert hc_item.assembly_index == 0
    assert item.item == "C1=CC=CC=C1"

async def _assert_assembled_input_rows(db_session, parent_job, input_jobs): 
    """
    The assembled variant feeds **one** HCInputJob whose HCInputItems table
    should contain *two* rows (assembly_index 0 and 1).
    """
    assert len(input_jobs) == 1
    stmt = (
        select(HCInputItems.assembly_index, Item.item)
        .join(Item)
        .where(HCInputItems.job_id == input_jobs[0].id)
        .order_by(HCInputItems.assembly_index)
    )
    rows = (await db_session.execute(stmt)).all()
    assert rows == [
        (0, "NCC"),   # assembly_index 0  (EN1)
        (1, "CCC"),   # assembly_index 1  (EN2)
    ]

# -----------------------------------------------------------------------------
# 1. STANDARD variant (no mapper, no assembly) - happy path
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_hc_job_standard(
    db_session,
    create_test_datasource_plugin,
    create_test_embedding,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    datasource, emb = await create_test_datasource_plugin()
    filter_p = await create_test_filter_plugin()
    score_p = await create_test_score_plugin()

    create_args = _hc_base_parts(
        embeddings=[emb],
        datasource_plugins=[datasource],
        filter_plugins=[filter_p],
        score_plugin=score_p,
        )
    data_configs = create_args.pop('data_configs')

    cfg = HCConfigCreate(
        **create_args,
        data_config=data_configs[0]
    )

    create_obj = HCJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[1.0, 2.0, 3.0],
        ),
        job_inputs=_mk_job_inputs(single=True),
    )

    _, _, parent_job, input_jobs = await create_hc_job(db_session, create_obj)

    await _assert_parent_and_input_rows(db_session, parent_job, input_jobs)

    stmt = select(JobPlugin.plugin_id).where(JobPlugin.job_id == parent_job.id)
    plugin_ids = {pid for (pid,) in (await db_session.execute(stmt)).all()}
    expected = {datasource.id, emb.id, filter_p.id, score_p.id}
    assert expected.issubset(plugin_ids)

    await db_session.commit()

# -----------------------------------------------------------------------------
# 2. MAPPER variant - happy path (no embedding_configs provided)
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_hc_job_mapper(
    db_session,
    create_test_mapper_plugin,
    create_test_assembly_plugin,
    create_test_datasource_plugin,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    # Mapper with 2 outputs
    mapper_plugin, input_emb, output_embs = await create_test_mapper_plugin(n_outputs=2)
    assembly_plugin = await create_test_assembly_plugin(num_parents=2)

    # One datasource per output embedding, each uses that embedding
    datasources = []
    for emb in output_embs:
        ds, _ = await create_test_datasource_plugin(embedding=emb)
        datasources.append(ds)

    filter_p = await create_test_filter_plugin()
    score_p = await create_test_score_plugin()

    cfg = HCMapperConfigCreate(
        **_hc_base_parts(
            embeddings=None,  # omit embedding_configs on purpose
            datasource_plugins=datasources,
            filter_plugins=[filter_p],
            score_plugin=score_p,
            mapper_plugin=mapper_plugin,
            assembly_plugin=assembly_plugin,
        ),
    )

    create_obj = HCMapperJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[0.5, 0.25, 0.125],
        ),
        job_inputs=_mk_job_inputs(single=True),
    )

    search_config, _, parent_job, input_jobs = await create_hc_job(db_session, create_obj)

    await _assert_parent_and_input_rows(db_session, parent_job, input_jobs)

    # Embeddings pulled automatically: input_emb + 2 output_embs
    auto_ids = {input_emb.id, *(e.id for e in output_embs)}
    assert auto_ids.issubset(set(search_config.embedding_dict.keys()))

    # and mapper plugin present in JobPlugin rows
    stmt = select(JobPlugin.plugin_id).where(JobPlugin.job_id == parent_job.id)
    plugin_ids = {pid for (pid,) in (await db_session.execute(stmt)).all()}
    expected = {p.id for p in datasources} | {mapper_plugin.id} | auto_ids | {filter_p.id, score_p.id}
    assert expected.issubset(plugin_ids)

    await db_session.commit()

# -----------------------------------------------------------------------------
# 3. ASSEMBLED variant - happy path (assembly with 2 parents)
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_hc_job_assembled(
    db_session,
    create_test_assembly_plugin,
    create_test_datasource_plugin,
    create_test_filter_plugin,
    create_test_score_plugin,
    create_test_embedding,
):
    assembly_plugin = await create_test_assembly_plugin(num_parents=2)

    # Two datasource plugins (assembly_index 0 & 1)
    ds0, emb0 = await create_test_datasource_plugin()
    ds1, emb1 = await create_test_datasource_plugin()

    filter_p = await create_test_filter_plugin()
    score_p = await create_test_score_plugin()

    cfg = HCAssembledConfigCreate(
        **_hc_base_parts(
            embeddings=None,  # omit embedding_configs
            datasource_plugins=[ds0, ds1],
            filter_plugins=[filter_p],
            score_plugin=score_p,
            assembly_plugin=assembly_plugin,
        ),
    )

    lr_cfg = [
        LearningRate(learning_rate=[1.0, 2.0, 3.0], assembly_index=0),
        LearningRate(learning_rate=[1.5, 2.5, 3.5], assembly_index=1),
    ]

    create_obj = HCAssembledJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCAssembledUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=lr_cfg,
        ),
        job_inputs=_mk_job_inputs(single=False),
    )

    search_config, _, parent_job, input_jobs = await create_hc_job(db_session, create_obj)
    await _assert_assembled_input_rows(db_session, parent_job, input_jobs)

    # assembly embedding pull: both datasource embeddings present
    assert {emb0.id, emb1.id}.issubset(search_config.embedding_dict.keys())

    # num_parents match datasources enforced by CRUD; reaching here implies success
    stmt = select(JobPlugin.plugin_id).where(JobPlugin.job_id == parent_job.id)
    plugin_ids = {pid for (pid,) in (await db_session.execute(stmt)).all()}
    expected = {ds0.id, ds1.id, emb0.id, emb1.id, assembly_plugin.id, filter_p.id, score_p.id}
    assert expected.issubset(plugin_ids)

    await db_session.commit()

# -----------------------------------------------------------------------------
# 4. Embedding override behaviour - custom override wins
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_embedding_override_params(
    db_session,
    create_test_datasource_plugin,
    create_test_embedding,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    # datasource + embedding
    datasource, emb = await create_test_datasource_plugin()
    filter_p = await create_test_filter_plugin()
    score_p = await create_test_score_plugin()

    # supply embedding_configs with override (timeout=123)
    emb_cfg = ExecutePluginCreate(
        plugin_id=emb.id,
        override_params=PluginOverrideParams(timeout=123),
    )

    create_args = _hc_base_parts(
            embeddings=[],  # placeholder - will be replaced below
            datasource_plugins=[datasource],
            filter_plugins=[filter_p],
            score_plugin=score_p,
        )
    _ = create_args.pop('embedding_configs')
    data_configs = create_args.pop('data_configs')
    cfg = HCConfigCreate(
        **create_args,
        embedding_configs=[emb_cfg],
        data_config=data_configs[0],
    )

    create_obj = HCJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[1, 2, 3],
        ),
        job_inputs=_mk_job_inputs(single=True),
    )

    search_config, _, parent_job, _ = await create_hc_job(db_session, create_obj)

    # embedding present
    assert emb.id in search_config.embedding_dict
    plugin_cfg = search_config.embedding_dict[emb.id]
    assert plugin_cfg.plugin.timeout == 123  # override applied

    # JobPlugin rows include emb id exactly once (dedup works) and datasource
    stmt = select(JobPlugin.plugin_id).where(JobPlugin.job_id == parent_job.id)
    plugin_ids = [pid for (pid,) in (await db_session.execute(stmt)).all()]
    assert plugin_ids.count(emb.id) == 1

    await db_session.commit()



# ----------------------------------------------------------------------------- 
# 5. NEGATIVE‑PATH TESTS 
# ----------------------------------------------------------------------------- 

# 5‑A  Duplicate learning‑rate indices ----------------------------------------

def test_duplicate_learning_rate_indices():
    with pytest.raises(PydanticValidationError):
        HCAssembledUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[
                LearningRate(learning_rate=[1, 2, 3], assembly_index=0),
                LearningRate(learning_rate=[1, 2, 3], assembly_index=0),   # duplicate
            ],
        )

# 5‑B  Duplicate assembly_index inside job inputs -----------------------------

def test_duplicate_job_input_indices():
    with pytest.raises(PydanticValidationError):
        HCAssembedInputItem(
            max_iterations=5,
            items=[
                {"external_id": "EN1", "item": "AAA", "assembly_index": 0},
                {"external_id": "EN2", "item": "BBB", "assembly_index": 0},  # duplicate
            ],
        )

# 5‑C  Mapper outputs vs datasource count mismatch ---------------------------

@pytest.mark.asyncio
async def test_mapper_datasource_count_mismatch_raises(
    db_session,
    create_test_mapper_plugin,
    create_test_assembly_plugin,
    create_test_datasource_plugin,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    # mapper with 3 outputs
    mapper, _, outs = await create_test_mapper_plugin(n_outputs=3)
    assembly_plugin = await create_test_assembly_plugin(num_parents=3)

    # only TWO datasources provided  → should raise ValidationError
    datasources = []
    for i in range(2):
        data_source, _ = await create_test_datasource_plugin(embedding=outs[i])
        datasources.append(data_source)

    filter_p = await create_test_filter_plugin()
    score_p = await create_test_score_plugin()

    cfg = HCMapperConfigCreate(
        **_hc_base_parts(
            embeddings=None,
            datasource_plugins=datasources,
            filter_plugins=[filter_p],
            score_plugin=score_p,
            mapper_plugin=mapper,
            assembly_plugin=assembly_plugin,
        ),
    )

    create_obj = HCMapperJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[0.1, 0.2, 0.3],
        ),
        job_inputs=_mk_job_inputs(single=True),
    )

    with pytest.raises(CrudValidationError):
        await create_hc_job(db_session, create_obj)

    await db_session.commit()

# 5‑D  Assembly parent count mismatch ----------------------------------------

@pytest.mark.asyncio
async def test_assembly_parent_count_mismatch_raises(
    db_session,
    create_test_assembly_plugin,
    create_test_datasource_plugin,
    create_test_filter_plugin,
    create_test_score_plugin,
):
    # assembly expects 3 parents, but we only give 2 datasources
    assembly_plugin = await create_test_assembly_plugin(num_parents=3)

    ds0, _ = await create_test_datasource_plugin()
    ds1, _ = await create_test_datasource_plugin()

    filter_p = await create_test_filter_plugin()
    score_p = await create_test_score_plugin()

    cfg = HCAssembledConfigCreate(
        **_hc_base_parts(
            embeddings=None,
            datasource_plugins=[ds0, ds1],   # only 2
            filter_plugins=[filter_p],
            score_plugin=score_p,
            assembly_plugin=assembly_plugin,
        ),
    )

    lr_cfg = [LearningRate(learning_rate=[1, 2], assembly_index=i) for i in (0, 1)]

    create_obj = HCAssembledJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCAssembledUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=lr_cfg,
        ),
        job_inputs=_mk_job_inputs(single=False),
    )

    with pytest.raises(CrudValidationError):
        await create_hc_job(db_session, create_obj)

    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_role", ["mapper", "assembly", "datasource", "filter", "score"])
async def test_wrong_plugin_type_raises(
    db_session,
    wrong_role,
    # common helpers
    create_test_filter_plugin,
    create_test_score_plugin,
    create_test_datasource_plugin,
    create_test_mapper_plugin,
    create_test_assembly_plugin,
    create_test_embedding,
):
    """
    Pass a plugin of an inappropriate type into each role and ensure
    create_hc_job raises AssertionError from validate_search_config_plugin_types().
    """

    # ---- Build the *correct* plugins/embeddings first -----------------------
    datasource, ds_emb = await create_test_datasource_plugin()
    filter_p        = await create_test_filter_plugin()
    score_p         = await create_test_score_plugin()
    mapper_p, _, mapper_outs = await create_test_mapper_plugin(n_outputs=2)
    assembly_p      = await create_test_assembly_plugin(num_parents=2)

    # by default we’ll construct a full “mapper” workflow (datasource+assembly+mapper);
    # we’ll swap ONE of those plugins for a wrong‑type replacement.
    if wrong_role == 'filter':
        wrong_plugin = await create_test_score_plugin()
    else:
        wrong_plugin = await create_test_filter_plugin()   # a FILTER plugin - wrong for every other slot

    # Helper to pick correct / wrong plugin per slot --------------------------
    def pick(role, correct):
        return wrong_plugin if role == wrong_role else correct

    # Build config pieces -----------------------------------------------------
    cfg = HCMapperConfigCreate(
        **_hc_base_parts(
            embeddings=None,
            datasource_plugins=[
                pick("datasource", datasource)  # only one datasource needed for failure
            ],
            filter_plugins=[pick("filter", filter_p)],
            score_plugin=pick("score", score_p),
            mapper_plugin=pick("mapper", mapper_p),
            assembly_plugin=pick("assembly", assembly_p),
        ),
    )

    create_obj = HCMapperJobCreate(
        job_params=HCJobParams(),
        plugin_config=cfg,
        update_params=HCUpdateParams(
            update_type=UpdateType.GLOBAL_UPDATE,
            distance_metric=DistanceMetric.Cosine,
            learning_rate=[0.1, 0.2, 0.3],
        ),
        job_inputs=_mk_job_inputs(single=True),
    )

    with pytest.raises(CrudValidationError):
        await create_hc_job(db_session, create_obj)

    await db_session.commit()