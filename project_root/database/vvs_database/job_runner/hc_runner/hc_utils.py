from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import product as _prod
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.crud.hc_crud.hc_job_create import load_search_config_plugins
from vvs_database.crud.hc_crud.hc_job_crud import load_hc_input_job_items
from vvs_database.crud.hc_crud.hc_results_crud import (
    upsert_hc_iteration_results,
    upsert_hc_results,
)
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops import (
    DecomposedDataOp,
    ItemOp,
    MapperDataOp,
    SingleDataOp,
)
from vvs_database.job_runner.hc_runner.hc_update import top_1_update
from vvs_database.models.job_models.hc_models import HCInputJob
from vvs_database.schemas.enums import JobStatus, TERMINAL_STATUSES
from vvs_database.schemas.hc_schemas import (
    HCAssembledUpdateParams,
    HCSearchConfigs,
    HCSearchIteration,
)
from vvs_database.schemas.internal_schemas import ExecutePlugin
from vvs_database import logging

################################################################################
# 1.  Pure helpers & utilities – no side‑effects                                #
################################################################################


def _over_time_limit(start_time: datetime, limit_s: Optional[int]) -> bool:
    if limit_s is None:
        return False
    return (datetime.now(tz=timezone.utc) - start_time).total_seconds() > limit_s


def _over_inference_limit(inference: int, limit: Optional[int]) -> bool:
    return limit is not None and inference > limit


def should_stop_input(
    job: HCInputJob,
    parent: HCInputJob,
    iterate_i: int,
    max_iter: int,
    new_queries: list | None,
) -> Tuple[bool, JobStatus]:
    """Return (should_stop, resulting_status)."""
    finish = iterate_i >= max_iter - 1
    invalid = not new_queries
    early = any(
        [
            _over_inference_limit(job.inference, job.inference_limit),
            _over_time_limit(job.started_at, job.time_limit),
            _over_inference_limit(parent.inference, parent.inference_limit),
            _over_time_limit(parent.started_at, parent.time_limit),
        ]
    )
    status = (
        JobStatus.COMPLETE
        if finish and not early and not invalid
        else JobStatus.COMPLETE_EARLY_STOP
    )
    return finish or invalid or early, status


async def should_finalize_parent(db: AsyncSession, parent_id: int) -> Optional[JobStatus]:
    """Return final status for parent if every child finished, else None."""
    res = await db.execute(select(HCInputJob.status).where(HCInputJob.parent_id == parent_id))
    statuses = {row[0] for row in res}
    if statuses <= TERMINAL_STATUSES:
        if {JobStatus.COMPLETE_WITH_ERRORS, JobStatus.FAILED} & statuses:
            return JobStatus.COMPLETE_WITH_ERRORS
        if JobStatus.COMPLETE_EARLY_STOP in statuses:
            return JobStatus.COMPLETE_EARLY_STOP
        return JobStatus.COMPLETE
    return None

################################################################################
# 2.  Lightweight wrappers around domain ops                                   #
################################################################################


class OpFactory:
    """Pure helper: search config → instantiated ops."""

    @staticmethod
    def build_data_op(cfg: HCSearchConfigs, connections: Connections, log_id: str):
        data_cfg_dict = {d.data_source_params.assembly_index: d for d in cfg.data_configs}
        if cfg.mapper_config is not None:
            mapper = cfg.mapper_config
            emb_dict = cfg.embedding_dict
            input_emb = emb_dict[mapper.plugin.input_embedding_id]
            output_embs = [emb_dict[eid.embedding_id] for eid in mapper.plugin.output_order]
            return MapperDataOp(
                mapper_config=mapper,
                input_embedding_config=input_emb,
                output_embedding_configs=output_embs,
                data_config_dict=data_cfg_dict,
                assembly_config=cfg.assembly_config,
                connections=connections,
                log_id=log_id,
            )
        if cfg.assembly_config is not None:
            return DecomposedDataOp(
                data_config_dict=data_cfg_dict,
                assembly_config=cfg.assembly_config,
                connections=connections,
                log_id=log_id,
            )
        return SingleDataOp(cfg.data_configs[0], connections=connections, log_id=log_id)

    @staticmethod
    def _build_item_op(
        plugin_cfg: ExecutePlugin,
        search_cfg: HCSearchConfigs,
        connections: Connections,
        log_id: str,
    ) -> ItemOp:
        plugin = plugin_cfg.plugin
        embedding_cfgs = []
        if plugin.type != "embedding" and plugin.embedding_ids:
            embedding_cfgs = [search_cfg.embedding_dict[eid] for eid in plugin.embedding_ids]
        return ItemOp(plugin_cfg, embedding_cfgs, connections, log_id)

    @classmethod
    def build_item_ops(
        cls, cfg: HCSearchConfigs, connections: Connections, log_id: str
    ) -> Tuple[List[ItemOp], ItemOp, Dict[int, List[ItemOp]]]:
        filter_ops = [cls._build_item_op(c, cfg, connections, log_id) for c in cfg.filter_configs]
        score_op = cls._build_item_op(cfg.score_config, cfg, connections, log_id)
        source_embed_ops = {}
        for asm_idx, src_embs in cfg.source_embeddings.items():
            source_embed_ops[asm_idx] = [cls._build_item_op(c, cfg, connections, log_id) for c in src_embs]
        return filter_ops, score_op, source_embed_ops

class IterationExecutor:
    """Wrapper that runs the data-filter-score pipeline for one query tuple."""

    def __init__(self, data_op, filter_ops: List[ItemOp], score_op: ItemOp, log_id: str):
        self.data_op, self.filter_ops, self.score_op = data_op, filter_ops, score_op
        self.log_id = log_id

    async def __call__(self, search_iter: HCSearchIteration):
        if search_iter.query is None:
            search_iter.set_query()
        items = await self.data_op(search_iter.query)
        if not items:
            logging.info(f"Data op failed to produce results")
            return [], None
        
        for f in self.filter_ops:
            items = await f(items)

        items = await self.score_op(items)
        search_iter.results = items
        valid = search_iter.get_results(deduplicate=True, only_valid=True)
        if not valid:
            return items, None
        return items, top_1_update(search_iter)

################################################################################
# 3.  IO helpers (DB writes)                                                   #
################################################################################


class ResultPersister:
    """Encapsulates result + iteration-result upserts."""

    def __init__(self, db: AsyncSession, log_id: str):
        self.db, self.log_id = db, log_id

    async def persist(self, parent_job_id: int, iteration_id: int, items, dup_counts):
        result_id_map = await upsert_hc_results(self.db, 
                                                job_id=parent_job_id, 
                                                items=list(items.values()), 
                                                batch_size=100)
        counts_by_result_id = {
            rid: dup_counts[f"{item.item_data.item_id}_{getattr(item.assembly_data, 'assembly_id', None)}"]
            for (item_id, asm_id), rid in result_id_map.items()
            for item in (items[f"{item_id}_{asm_id}"],)
        }
        await upsert_hc_iteration_results(self.db, iteration_id=iteration_id, counts_by_result=counts_by_result_id)

################################################################################
# 4.  State container                                                          #
################################################################################

@dataclass
class JobContext:
    db: AsyncSession
    runner_log_id: str
    job: HCInputJob | None = None
    parent: HCInputJob | None = None
    input_items: dict = field(default_factory=dict)
    search_cfg: Optional[HCSearchConfigs] = None
    update_params: Optional[HCAssembledUpdateParams] = None

    async def load(self, job: HCInputJob):
        self.job = job
        await self.db.refresh(job, ["parent"])
        self.parent = job.parent
        self.input_items = await load_hc_input_job_items(self.db, job)
        self.update_params = HCAssembledUpdateParams(**job.job_json["update_params"])
        self.search_cfg = await load_search_config_plugins(self.db, HCSearchConfigs(**job.job_json["search_config"]))
        await self.db.commit()
