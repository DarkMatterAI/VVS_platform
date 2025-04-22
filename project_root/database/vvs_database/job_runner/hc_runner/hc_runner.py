from __future__ import annotations

from itertools import product
from typing import Dict, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database import logging
from vvs_database.crud.hc_crud.hc_job_crud import latest_hc_iteration
from vvs_database.crud.hc_crud.hc_results_crud import (
    sum_inference_for_hc_input_job,
    sum_inference_for_hc_job,
)
from vvs_database.crud.job_crud import create_job, update_helper
from vvs_database.execution.connections import Connections
from vvs_database.execution.ops import ItemOp
from vvs_database.job_runner.base_runner import JobRunner
from vvs_database.job_runner.hc_runner.hc_utils import (
    OpFactory,
    IterationExecutor,
    ResultPersister,
    JobContext,
    should_stop_input,
    should_finalize_parent
)

from vvs_database.models.job_models.hc_models import HCIterationJob
from vvs_database.schemas.enums import JobStatus, JobType, TERMINAL_STATUSES
from vvs_database.schemas.hc_schemas import HCSearchIteration
from vvs_database.schemas.internal_schemas import GradientEmbedding


################################################################################
# The orchestrator                                                             #
################################################################################


class HCRunner(JobRunner):
    """High-level orchestration for HC input jobs."""

    ###############
    # Life‑cycle  #
    ###############

    async def load_job(self, db_session: AsyncSession):
        await super().load_job(db_session)
        self.ctx = JobContext(db_session, self.log_id)
        await self.ctx.load(self.job)

    def load_ops(self, connections: Connections):
        cfg = self.ctx.search_cfg
        self.data_op = OpFactory.build_data_op(cfg, connections, self.log_id)
        self.filter_ops, self.score_op = OpFactory.build_item_ops(cfg, connections, self.log_id)
        self.score_op.last_executed_count = 0

    async def init_job(self, connections: Connections):
        logging.info(f"{self.log_id}: Embedding inputs & creating initial queries")
        await self._embed_inputs(connections)
        self.initial_queries = self._build_initial_queries()

    async def init_first_iteration(self, db_session: AsyncSession):
        logging.info(f"{self.log_id}: Initializing first iteration")
        self.ctx.parent = await update_helper(self.ctx.parent, {"status": JobStatus.RUNNING})
        self.ctx.job = await update_helper(self.ctx.job, {"status": JobStatus.RUNNING,
                                                          "dagster_run_id": self.ctx.parent.dagster_run_id})
        iter_job = await self._create_iteration_job(db_session, 0, self.initial_queries)
        await db_session.commit()
        return iter_job

    ######################
    #   Public API call  #
    ######################

    async def __call__(self, connections: Connections):
        db = self.ctx.db

        iter_job = await self._fetch_latest_iteration(db)
        if iter_job is None:
            logging.info(f"{self.log_id}: Next iteration not found, aborting")
            return None
        
        logging.info(f"{self.log_id}: Starting next iteration - {iter_job.iteration}")

        uniq_items, dup_counts, new_queries = await self._run_iteration(iter_job, connections)

        await self._persist_iteration_results(iter_job, uniq_items, dup_counts)
        await self._update_inference_stats(iter_job)

        next_iter = await self._maybe_create_next_iteration(iter_job, new_queries)
        await db.commit() # required for parent finalize check to work
        await self._maybe_finalize_parent()

        await db.commit()
        return next_iter

    ######################
    #   Helper methods   #
    ######################

    async def _fetch_latest_iteration(self, db: AsyncSession) -> Optional[HCIterationJob]:
        latest_iter: HCIterationJob = await latest_hc_iteration(db, self.job_id)
        if latest_iter.status in TERMINAL_STATUSES:
            logging.info(f"{self.log_id}: Found completed iteration, aborting")
            return None
        return await update_helper(latest_iter, {"status": JobStatus.RUNNING})

    async def _run_iteration(self, iter_job: HCIterationJob, connections: Connections):
        executor = IterationExecutor(self.data_op, self.filter_ops, self.score_op, self.log_id)
        search_iters = self._hc_iter_to_search_iters(iter_job)
        new_queries: List[List[GradientEmbedding]] | List = []
        dup_counter = {}
        unique_items: Dict[str, object] = {}
        for s_iter in search_iters:
            items, nq = await executor(s_iter)
            if nq:
                new_queries.append(nq)
            self._accumulate(items, unique_items, dup_counter)
        return unique_items, dup_counter, new_queries

    async def _persist_iteration_results(self, iter_job: HCIterationJob, items, dup_counts):
        await ResultPersister(self.ctx.db, self.log_id).persist(
            parent_job_id=self.ctx.parent.id,
            iteration_id=iter_job.id,
            items=items,
            dup_counts=dup_counts,
        )

    async def _update_inference_stats(self, iter_job: HCIterationJob):
        db = self.ctx.db
        iter_job = await update_helper(iter_job, {"status": JobStatus.COMPLETE, 
                                                  "inference": self.score_op.last_executed_count})
        self.ctx.job = await update_helper(self.ctx.job, 
                                           {"inference": await sum_inference_for_hc_input_job(db, self.ctx.job.id)})
        self.ctx.parent = await update_helper(self.ctx.parent, 
                                              {"inference": await sum_inference_for_hc_job(db, self.ctx.parent.id)})

    async def _maybe_create_next_iteration(self, iter_job: HCIterationJob, new_queries):
        stop, new_status = should_stop_input(
            self.ctx.job,
            self.ctx.parent,
            iter_job.iteration,
            self.ctx.job.max_iterations,
            new_queries,
        )
        if stop:
            await update_helper(self.ctx.job, {"status": new_status})
            logging.info(f"{self.log_id}: Iterations complete ({new_status})")
            return None
        return await self._create_iteration_job(self.ctx.db, iter_job.iteration + 1, new_queries, iter_job.id)

    async def _maybe_finalize_parent(self):
        final_status = await should_finalize_parent(self.ctx.db, self.ctx.parent.id)
        if final_status:
            await update_helper(self.ctx.parent, {"status": final_status})

    ###############
    # Build steps #
    ###############

    async def _embed_inputs(self, connections: Connections):
        for asm_idx, item in self.ctx.input_items.items():
            for emb_cfg in self.ctx.search_cfg.source_embeddings[asm_idx]:
                await ItemOp(emb_cfg, [], connections, self.log_id)([item])

    def _build_initial_queries(self):
        lr = {lr_cfg.assembly_index: lr_cfg.learning_rate for lr_cfg in self.ctx.update_params.learning_rate}
        query_dict: Dict[int, List[GradientEmbedding]] = {}
        for asm_idx, item in self.ctx.input_items.items():
            query_dict[asm_idx] = [
                GradientEmbedding(
                    **emb.model_dump(),
                    learning_rates=lr[asm_idx],
                    gradient=None,
                    assembly_index=asm_idx,
                )
                for emb in item.embeddings.values()
            ]
        return list(product(*query_dict.values()))

    async def _create_iteration_job(
        self,
        db_session: AsyncSession,
        iteration: int,
        queries: List[Tuple[GradientEmbedding]],
        parent_id: int | None = None,
    ):
        extra_args = {
            "input_id": self.ctx.job.id,
            "parent_id": parent_id,
            "iteration": iteration,
            "query_embedding": {"query": [tuple([i.model_dump() for i in q]) for q in queries]},
        }
        return await create_job(
            db_session,
            job_type=JobType.HILL_CLIMB_JOB_ITERATION,
            job_json=None,
            auto_execute=self.ctx.job.auto_execute,
            dagster_run_id=self.ctx.job.dagster_run_id,
            extra_args=extra_args,
        )

    def _hc_iter_to_search_iters(self, rec: HCIterationJob):
        qs = [tuple(GradientEmbedding(**e) for e in tup) for tup in rec.query_embedding["query"]]
        return [HCSearchIteration(update_params=self.ctx.update_params, query_embeddings=q) for q in qs]

    @staticmethod
    def _accumulate(items, uniq: dict, dup: dict):
        for item in items:
            key = f"{item.item_data.item_id}_{getattr(item.assembly_data, 'assembly_id', None)}"
            uniq.setdefault(key, item)
            dup[key] = dup.get(key, 0) + 1

