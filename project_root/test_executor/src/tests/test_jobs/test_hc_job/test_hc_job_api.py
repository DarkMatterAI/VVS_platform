import pytest
from sqlalchemy import select, func
from itertools import islice

from tests.utils.backend_utils import backend_get_plugins_by_filter

from vvs_database.models import HCResult
from vvs_database.execution.connections import get_connections
from vvs_database.job_runner.hc_runner.hc_runner import HCRunner
from vvs_database import crud, schemas 
from vvs_database.schemas import DistanceMetric, PluginType
from vvs_database.schemas.hc_schemas import (
    UpdateType,
)

# ------------- helpers ------------------------------------------------------

def _pick_first(plugins, typ):
    for p in plugins:
        if p["type"] == typ:
            return p
    pytest.skip(f"No plugin of type {typ!r} registered in backend")

def _pick_n(plugins, typ, n):
    sel = [p for p in plugins if p["type"] == typ][:n]
    if len(sel) < n:
        pytest.skip(f"Need {n} plugins of type {typ}")
    return sel

async def _run_runner_and_assert(db, parent_job_id, input_job_id):
    conns  = get_connections(db)
    runner = HCRunner(job_id=input_job_id)

    await runner.load_job(db)
    runner.load_ops(conns)
    await runner.init_job(conns)
    await runner.init_first_iteration(db)
    await db.commit()

    # run until done (cap iterations for safety)
    for _ in range(20):
        nxt = await runner(conns)
        if nxt is None:
            break
    else:
        pytest.fail("Runner did not converge in 20 iterations")

    # await conns.close()
    # await db.commit()

    # at least one result persisted for this parent
    nres = await db.scalar(
        select(func.count()).select_from(HCResult).where(HCResult.job_id == parent_job_id)
    )
    assert nres and nres > 0
    await conns.close()
    await db.commit()


# ------------- 1) STANDARD variant ------------------------------------------

@pytest.mark.asyncio
async def test_api_hc_job_standard_create_and_fetch(
    db_session,
    backend_client,
    job_cleanup,      # to cleanup parent after test
):
    # Discover mock plugins exposed by backend
    plugins     = backend_get_plugins_by_filter(backend_client, "mock_%")
    datasource  = _pick_first(plugins, "data_source")
    filter_plg  = _pick_first(plugins, "filter")
    score_plg   = _pick_first(plugins, "score")
    emb_plg     = await crud.get_plugin(db_session, datasource["embedding_ids"][0], response_model=True)

    # Build HTTP payload (HCJobCreate)
    payload = {
        "job_params": {"auto_execute": False},
        "plugin_config": {
            "filter_configs": [{"plugin_id": filter_plg["id"]}],
            "score_config":   {"plugin_id": score_plg["id"]},
            "embedding_configs": [{"plugin_id": emb_plg.id}],
            "data_config": {
                "plugin_id": datasource["id"],
                "data_source_params": {"k": 3, "assembly_index": 0},
            },
        },
        "update_params": {
            "update_type": UpdateType.GLOBAL_UPDATE.value,
            "distance_metric": DistanceMetric.Cosine.value,
            "learning_rate": [1.0, 2.0, 3.0],
        },
        "job_inputs": [
            {
                "item": {"external_id": "Z1", "item": "C1=CC=CC=C1"},
                "max_iterations": 2,
            }
        ],
    }

    # Create via API
    r = backend_client.post("/api/v1/hc_jobs", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["success"] is True
    parent_id = data["parent_job_id"]
    input_ids = data["input_job_ids"]
    assert input_ids and len(input_ids) == 1
    job_cleanup({"id": parent_id})  # schedule cleanup

    # Produce results by running the runner locally
    await _run_runner_and_assert(db_session, parent_id, input_ids[0])

    # Fetch results via API
    r2 = backend_client.get(f"/api/v1/hc_jobs/{parent_id}/results", params={"order_by": "score", "limit": 100})
    assert r2.status_code == 200, r2.text
    out = r2.json()
    assert out["success"] is True and out["hc_job_id"] == parent_id
    assert out["results"], "API returned empty results"
    assert out["results"][0]["score"] is not None


# ------------- 2) ASSEMBLED variant -----------------------------------------

@pytest.mark.asyncio
async def test_api_hc_job_assembled_create_and_fetch(
    db_session,
    backend_client,
    job_cleanup,
):
    plugins      = backend_get_plugins_by_filter(backend_client, "mock_%")
    assembly_plg = _pick_first(plugins, "assembly")
    datasource  = _pick_first(plugins, "data_source")
    datasources = [datasource, datasource]
    # datasources  = _pick_n(plugins, "data_source", 1)
    filter_plg   = _pick_first(plugins, "filter")
    score_plg    = _pick_first(plugins, "score")

    payload = {
        "job_params": {"auto_execute": False},
        "plugin_config": {
            "filter_configs": [{"plugin_id": filter_plg["id"]}],
            "score_config":   {"plugin_id": score_plg["id"]},
            "data_configs": [
                {"plugin_id": datasources[0]["id"], "data_source_params": {"k": 3, "assembly_index": 0}},
                {"plugin_id": datasources[1]["id"], "data_source_params": {"k": 3, "assembly_index": 1}},
            ],
            "assembly_config": {"plugin_id": assembly_plg["id"]},
        },
        "update_params": {
            "update_type": UpdateType.GLOBAL_UPDATE.value,
            "distance_metric": DistanceMetric.Cosine.value,
            "learning_rate": [
                {"assembly_index": 0, "learning_rate": [1.0, 2.0]},
                {"assembly_index": 1, "learning_rate": [1.5, 2.5]},
            ],
        },
        "job_inputs": [
            {
                "items": [
                    {"external_id": "EN1", "item": "NCC", "assembly_index": 0},
                    {"external_id": "EN2", "item": "CCC", "assembly_index": 1},
                ],
                "max_iterations": 2,
            }
        ],
    }

    r = backend_client.post("/api/v1/hc_jobs", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()
    parent_id = data["parent_job_id"]
    input_ids = data["input_job_ids"]
    job_cleanup({"id": parent_id})

    await _run_runner_and_assert(db_session, parent_id, input_ids[0])

    r2 = backend_client.get(f"/api/v1/hc_jobs/{parent_id}/results", params={"order_by": "score", "limit": 200})
    assert r2.status_code == 200, r2.text
    out = r2.json()
    assert out["success"] and out["results"]
    assert out["results"][0]["score"] is not None


# ------------- 3) MAPPER variant --------------------------------------------

@pytest.mark.asyncio
async def test_api_hc_job_mapper_create_and_fetch(
    db_session,
    backend_client,
    plugin_cleanup,
    job_cleanup,
):
    plugins      = backend_get_plugins_by_filter(backend_client, "mock_%")
    mapper_plg   = _pick_first(plugins, "mapper")
    assembly_plg = _pick_first(plugins, "assembly")
    filter_plg   = _pick_first(plugins, "filter")
    score_plg    = _pick_first(plugins, "score")

    # Use one datasource replicated for each mapper output index
    out_idxs    = [o["index"] for o in mapper_plg["output_order"]]
    ds          = _pick_first(plugins, "data_source")
    datasources = [ds for _ in out_idxs]

    # Create a *new* mapper plugin in DB whose output_order aligns with those datasources
    output_order = [{"index": i, "embedding_id": datasources[i]["embedding_ids"][0]} for i in range(len(datasources))]
    mapper_create = schemas.MapperPluginCreate(
        name=mapper_plg["name"] + "_tmp",
        plugin_class=mapper_plg["plugin_class"],
        type=PluginType.MAPPER,
        execution_type=mapper_plg["execution_type"],
        group_key=mapper_plg["group_key"],
        timeout=mapper_plg["timeout"],
        max_concurrency=mapper_plg["max_concurrency"],
        max_retries=mapper_plg["max_retries"],
        batch_size=mapper_plg["batch_size"],
        endpoint_url=mapper_plg.get("endpoint_url"),
        input_embedding_id=mapper_plg["input_embedding_id"],
        output_order=output_order,
    )
    mapper_new = await crud.create_plugin(db_session, mapper_create, response_model=True)
    plugin_cleanup(mapper_new.model_dump())

    payload = {
        "job_params": {"auto_execute": False},
        "plugin_config": {
            "filter_configs": [{"plugin_id": filter_plg["id"]}],
            "score_config":   {"plugin_id": score_plg["id"]},
            "mapper_config":  {"plugin_id": mapper_new.id},
            "assembly_config":{"plugin_id": assembly_plg["id"]},
            "data_configs": [
                {"plugin_id": datasources[i]["id"], "data_source_params": {"k": 3, "assembly_index": i}}
                for i in range(len(datasources))
            ],
        },
        "update_params": {
            "update_type": UpdateType.GLOBAL_UPDATE.value,
            "distance_metric": DistanceMetric.Cosine.value,
            "learning_rate": [0.8, 0.4, 0.2],
        },
        "job_inputs": [
            {
                "item": {"external_id": "ZMAP", "item": "C1=CC=CC=C1"},
                "max_iterations": 2,
            }
        ],
    }

    r = backend_client.post("/api/v1/hc_jobs", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()
    parent_id = data["parent_job_id"]
    input_ids = data["input_job_ids"]
    job_cleanup({"id": parent_id})

    await _run_runner_and_assert(db_session, parent_id, input_ids[0])

    r2 = backend_client.get(f"/api/v1/hc_jobs/{parent_id}/results", params={"order_by": "score", "limit": 200})
    assert r2.status_code == 200, r2.text
    out = r2.json()
    assert out["success"] and out["results"]
    assert out["results"][0]["score"] is not None
