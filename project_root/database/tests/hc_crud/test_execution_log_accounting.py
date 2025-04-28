import types
from copy import deepcopy

import pytest
from sqlalchemy import select

# ── schemas & helpers we need ───────────────────────────────────────────────
from vvs_database.schemas.internal_schemas import ExecuteStats, ExecutionLog, ExecuteParams
from vvs_database.schemas.hc_schemas       import (
    HCJobParams, HCJobCreate, HCUpdateParams, UpdateType
)
from vvs_database.schemas                  import DistanceMetric
from vvs_database.crud.hc_crud            import create_hc_job
from vvs_database.crud.hc_crud.hc_results_crud import sum_inference_for_hc_input_job
from vvs_database.execution.connections    import Connections

# ORM tables for assertions
from vvs_database.models.job_models.hc_models import HCIterationJob


# ════════════════════════════════════════════════════════════════════════════
# helpers – build a 2-iteration HC job and run the patched runner
# ════════════════════════════════════════════════════════════════════════════
async def _make_two_iter_job(
    db,
    create_hc_job_standard,
    create_test_score_plugin,
    monkeypatch,
):
    # ---- 1) build a plain “standard” HC job (max_iterations=2) -------------
    _, _, parent, [input_job] = await create_hc_job_standard(max_iterations=2)
    score_plugin              = await create_test_score_plugin()

    # ---- 2) patch HCRunner so it needs no infra & emits fixed logs ---------
    from vvs_database.job_runner.hc_runner.hc_runner import HCRunner

    # dummy score-op with plugin_id
    dummy_score_cfg = types.SimpleNamespace(plugin_id=score_plugin.id)
    dummy_score_op  = types.SimpleNamespace(plugin_config=dummy_score_cfg)

    # iteration-counter → num_executed :  1st → 5,  2nd → 3
    def _build_log(num):
        stats = ExecuteStats(num_executed=num)
        return ExecutionLog(
            plugin_id=score_plugin.id,
            execute_params=ExecuteParams(),
            execute_stats=stats,
        ).model_dump()

    # patched _collect_execution_logs
    def fake_collect(self):
        n = getattr(self, "_iter_no", 0)
        self._iter_no = n + 1
        executed = 5 if n == 0 else 3
        return {score_plugin.id: _build_log(executed)}

    # patched _run_iteration → returns one InternalItem; stops immediately
    async def fake_run_iter(self, iter_job, *_):
        item = types.SimpleNamespace(
            item_data=types.SimpleNamespace(item_id=0, item="X"),
            assembly_data=None, valid=True, score=1.0,
            embeddings={}, query_group=None,
        )
        return { "0_None": item }, { "0_None": 1 }, []      # → new_queries empty

    # load_ops injects dummy score_op; others no-ops with collect_execution_logs()
    def fake_load_ops(self, *_):
        self.data_op    = types.SimpleNamespace(collect_execution_logs=lambda: {})
        self.filter_ops = []
        self.score_op   = dummy_score_op

    # init_job: skip embeddings; seed one empty query tuple
    async def fake_init_job(self, *_):
        self.initial_queries = [tuple()]

    monkeypatch.setattr(HCRunner, "load_ops",               fake_load_ops)
    monkeypatch.setattr(HCRunner, "init_job",               fake_init_job)
    monkeypatch.setattr(HCRunner, "_run_iteration",         fake_run_iter)
    monkeypatch.setattr(HCRunner, "_collect_execution_logs", fake_collect)

    # ---- 3) run the runner until
