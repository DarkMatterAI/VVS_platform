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

    async def init_job(self, connections: Connections):
        logging.info(f"{self.log_id}: Embedding inputs & creating initial queries")
        await self._embed_inputs(connections)
        self.initial_queries = self._build_initial_queries()

    async def init_first_iteration(self, db_session: AsyncSession):
        logging.info(f"{self.log_id}: Initializing first iteration")
        self.ctx.parent = await update_helper(self.ctx.parent, {"status": JobStatus.RUNNING})
        self.ctx.job = await update_helper(self.ctx.job, {"status": JobStatus.RUNNING})
        iter_job = await self._create_iteration_job(db_session, 0, self.initial_queries)
        await db_session.commit()
        return iter_job

    ######################
    #   Public API call  #
    ######################

    async def __call__(self, connections: Connections):
        logging.info(f"{self.log_id}: Starting next iteration")
        db = self.ctx.db

        iter_job = await self._fetch_latest_iteration(db)
        if iter_job is None:
            return None

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







# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy import select 
# import itertools
# from typing import List, Tuple 
# from datetime import datetime, timezone 

# from vvs_database import logging 
# from vvs_database.job_runner.base_runner import JobRunner
# from vvs_database.models.job_models.hc_models import HCInputJob, HCIterationJob
# from vvs_database.execution.connections import Connections
# from vvs_database.execution.ops import (
#     MapperDataOp,
#     DecomposedDataOp,
#     SingleDataOp,
#     ItemOp
# )
# from vvs_database.crud.job_crud import update_helper, create_job
# from vvs_database.crud.hc_crud.hc_job_create import load_search_config_plugins
# from vvs_database.crud.hc_crud.hc_job_crud import load_hc_input_job_items, latest_hc_iteration
# from vvs_database.crud.hc_crud.hc_results_crud import (
#     upsert_hc_results, 
#     upsert_hc_iteration_results,
#     sum_inference_for_hc_input_job,
#     sum_inference_for_hc_job
# )
# from vvs_database.schemas.hc_schemas import (
#     HCAssembledUpdateParams,
#     HCSearchConfigs,
#     HCSearchIteration
# )
# from vvs_database.schemas.internal_schemas import (
#     ExecutePlugin,
#     GradientEmbedding
# )
# from vvs_database.schemas.enums import JobStatus, JobType, TERMINAL_STATUSES
# from vvs_database.job_runner.hc_runner.hc_update import top_1_update

# def _over_time_limit(j) -> bool:
#     if j.time_limit is None:
#         return False
    
#     now = datetime.now(tz=timezone.utc)
#     elapsed = (now - j.started_at).total_seconds()
#     return elapsed > j.time_limit

# def _over_inference_limit(j) -> bool:
#     return j.inference_limit is not None and j.inference > j.inference_limit


# def hc_iteration_to_search_iteration(hc_iteration_record: HCIterationJob,
#                                      update_params: HCAssembledUpdateParams):
#     queries = hc_iteration_record.query_embedding['query']
#     queries = [tuple([GradientEmbedding(**i) for i in q]) for q in queries]
#     search_iterations = [HCSearchIteration(update_params=update_params,
#                                            query_embeddings=query)
#                          for query in queries]
#     return search_iterations

# class HCRunner(JobRunner):            
#     async def load_job(self, db_session: AsyncSession):
#         await super().load_job(db_session)
#         self.input_items = await load_hc_input_job_items(db_session, self.job) 
#         await db_session.refresh(self.job, ["parent"])   
#         self.parent_job = self.job.parent
#         job_json = self.job.job_json
#         self.update_params = HCAssembledUpdateParams(**job_json['update_params'])
#         self.search_config = HCSearchConfigs(**job_json['search_config'])
#         self.search_config = await load_search_config_plugins(db_session, self.search_config)
#         await db_session.commit()

#     def load_data_op(self, connections: Connections):
#         data_config_dict = {i.data_source_params.assembly_index:i 
#                             for i in self.search_config.data_configs}
#         mapper_config = self.search_config.mapper_config
#         assembly_config = self.search_config.assembly_config
#         if mapper_config is not None:
#             embedding_dict = self.search_config.embedding_dict
#             plugin = mapper_config.plugin
#             input_embedding_config = embedding_dict[plugin.input_embedding_id]
#             output_embedding_configs = [embedding_dict[i.embedding_id] for i in plugin.output_order]
            
#             self.data_op = MapperDataOp(mapper_config=mapper_config,
#                                         input_embedding_config=input_embedding_config,
#                                         output_embedding_configs=output_embedding_configs,
#                                         data_config_dict=data_config_dict,
#                                         assembly_config=assembly_config,
#                                         connections=connections,
#                                         log_id=self.log_id)
            
#         elif assembly_config is not None:
#             self.data_op = DecomposedDataOp(data_config_dict=data_config_dict,
#                                             assembly_config=assembly_config,
#                                             connections=connections,
#                                             log_id=self.log_id)
#         else:
#             self.data_op = SingleDataOp(data_config=self.search_config.data_configs[0],
#                                         connections=connections,
#                                         log_id=self.log_id)
            
#     def load_item_op(self, plugin_config: ExecutePlugin, connections: Connections):
#         embedding_configs = []
#         plugin = plugin_config.plugin
#         if (plugin.type != 'embedding') and (plugin.embedding_ids is not None):
#             embedding_configs = [self.search_config.embedding_dict[i] 
#                                  for i in plugin.embedding_ids]
#         item_op = ItemOp(plugin_config, embedding_configs, connections, self.log_id)
#         return item_op
    
#     def load_item_ops(self, connections: Connections):
#         self.filter_ops = [self.load_item_op(i, connections) 
#                            for i in self.search_config.filter_configs]
#         self.score_op = self.load_item_op(self.search_config.score_config, connections)
        
#     def load_ops(self, connections: Connections):
#         self.load_data_op(connections)
#         self.load_item_ops(connections)
        
#     async def embed_inputs(self, connections: Connections):
#         logging.info(f"{self.log_id}: Embedding inputs")
#         for assembly_index, item in self.input_items.items():
#             for embedding_config in self.search_config.source_embeddings[assembly_index]:
#                 item_op = ItemOp(embedding_config, [], connections, self.log_id)
#                 _ = await item_op([item])
                
#     async def init_job(self, connections: Connections):
#         logging.info(f"{self.log_id}: Creating initial queries")
#         await self.embed_inputs(connections)
#         query_dict = {}
#         lr_dict = {i.assembly_index : i.learning_rate for i in self.update_params.learning_rate}
#         for assembly_index, item in self.input_items.items():
#             query_dict[assembly_index] = []
#             for embedding_id, embedding in item.embeddings.items():
#                 query = GradientEmbedding(**embedding.model_dump(),
#                                           learning_rates=lr_dict[assembly_index],
#                                           gradient=None,
#                                           assembly_index=assembly_index)
#                 query_dict[assembly_index].append(query)
                
#         initial_queries = list(itertools.product(*query_dict.values()))
#         self.initial_queries = initial_queries
        
#     async def init_first_iteration(self, db_session: AsyncSession):
#         logging.info(f"{self.log_id}: Initializing first iteration")
#         self.parent_job = await update_helper(self.parent_job, {'status' : JobStatus.RUNNING})
#         self.job = await update_helper(self.job, {'status' : JobStatus.RUNNING})
#         job_iteration = await self.create_iteration_job(db_session, 0, self.initial_queries)
#         await db_session.commit()
#         return job_iteration
        
#     async def create_iteration_job(self, 
#                                    db_session: AsyncSession,
#                                    iteration: int, 
#                                    queries: List[Tuple[GradientEmbedding]], 
#                                    parent_id: int=None):
#         extra_args = {
#             'input_id' : self.job.id,
#             'parent_id' : parent_id,
#             'iteration' : iteration,
#             'query_embedding' : {'query' : [tuple([i.model_dump() for i in q]) for q in queries]}
#         }
        
#         job_iteration = await create_job(db_session,
#                                          job_type=JobType.HILL_CLIMB_JOB_ITERATION,
#                                          job_json=None,
#                                          auto_execute=self.job.auto_execute,
#                                          dagster_run_id=self.job.dagster_run_id,
#                                          extra_args=extra_args)
#         return job_iteration
        
#     async def execute_ops(self, search_iteration: HCSearchIteration):
#         if search_iteration.query is None:
#             search_iteration.set_query()
            
#         items = await self.data_op(search_iteration.query)
#         if len(items) == 0:
#             return search_iteration, None
        
#         for filter_op in self.filter_ops:
#             items = await filter_op(items)
            
# #         print(self.score_op.last_executed_count)
#         items = await self.score_op(items)
#         print(self.score_op.last_executed_count)
        
#         search_iteration.results = items
        
#         valid_results = search_iteration.get_results(deduplicate=True, only_valid=True)
#         if len(valid_results) == 0:
#             return search_iteration, None
        
#         new_queries = top_1_update(search_iteration)
#         return search_iteration, new_queries

#     async def __call__(self, connections: Connections):
#         logging.info(f"{self.log_id}: Starting next iteration")
#         self.load_ops(connections)
#         db_session = connections.db_service.db
#         latest_iteration = await latest_hc_iteration(db_session, self.job_id)
        
#         if latest_iteration.status in TERMINAL_STATUSES:
#             logging.info(f"{self.log_id}: Found completed iteration, aborting")
#             await db_session.commit()
#             return None
        
#         latest_iteration = await update_helper(latest_iteration, {'status' : JobStatus.RUNNING})
#         await db_session.commit()

#         logging.info(f"{self.log_id}: Starting Op Execution for iteration {latest_iteration.iteration}")
#         search_iterations = hc_iteration_to_search_iteration(latest_iteration, self.update_params)
#         new_queries = []
#         for search_iteration in search_iterations:
#             _, nq = await self.execute_ops(search_iteration)
#             if nq is not None:
#                 new_queries.append(nq)
            
#         logging.info(f"{self.log_id}: Processing Results")
#         unique_results = {}
#         dup_counter = {}
#         for item in search_iteration.results:
#             item_id = item.item_data.item_id
#             assembly_id = item.assembly_data.assembly_id if item.assembly_data else None
#             key = f"{item_id}_{assembly_id}"
#             unique_results[key] = item
#             dup_counter[key] = dup_counter.get(key, 0) + 1

#         results = list(unique_results.values())

#         result_id_map = await upsert_hc_results(
#             db_session,
#             job_id=self.job.parent_id,           # <-- HCJob.id
#             items=results,
#         )

#         counts_by_result_id = {}
#         for (item_id, assembly_id), result_id in result_id_map.items():
#             counts_by_result_id[result_id] = dup_counter[f"{item_id}_{assembly_id}"]
    
#         await upsert_hc_iteration_results(
#             db_session,
#             iteration_id=latest_iteration.id,
#             counts_by_result=counts_by_result_id,
#         )

#         logging.info(f"{self.log_id}: Updating Records")
#         latest_iteration = await update_helper(latest_iteration, 
#                                                {'status' : JobStatus.COMPLETE,
#                                                 'inference' : self.score_op.last_executed_count})

#         input_inference = await sum_inference_for_hc_input_job(db_session, self.job.id)
#         self.job = await update_helper(self.job, {'inference' : input_inference})

#         job_inference = await sum_inference_for_hc_job(db_session, self.parent_job.id)
#         self.parent_job = await update_helper(self.parent_job, {'inference' : job_inference})

#         logging.info(f"{self.log_id}: Checking next iteration")
#         finish_input = False
#         early_stop_input = False
#         invalid_queries = False

#         # iteration-limit check
#         if (latest_iteration.iteration) == self.job.max_iterations - 1:
#             finish_input = True

#         # valid queries check
#         if not new_queries:
#             invalid_queries = True

#         # inference / time-limit checks
#         now = datetime.now(tz=timezone.utc)

#         if _over_inference_limit(self.job) or _over_time_limit(self.job):
#             early_stop_input = True
#         elif _over_inference_limit(self.parent_job) or _over_time_limit(self.parent_job):
#             early_stop_input = True
    
#         next_iteration = None
#         if finish_input or early_stop_input or invalid_queries:
#             new_status = (
#                 JobStatus.COMPLETE
#                 if finish_input and (not early_stop_input) and (not invalid_queries)
#                 else JobStatus.COMPLETE_EARLY_STOP
#             )
#             self.job = await update_helper(self.job, {"status": new_status})
#             logging.info(f"{self.log_id}: Iterations complete")

#         else:
#             logging.info(f"{self.log_id}: Creating next iteration")
#             next_iteration = await self.create_iteration_job(db_session,
#                                                              latest_iteration.iteration+1,
#                                                              new_queries,
#                                                              latest_iteration.id)
    
#         rows = await db_session.execute(
#             select(HCInputJob.status).where(HCInputJob.parent_id == self.parent_job.id)
#         )
#         statuses = {row[0] for row in rows}

#         if statuses <= TERMINAL_STATUSES:                     # all inputs finished
#             if (JobStatus.COMPLETE_WITH_ERRORS in statuses) or (JobStatus.FAILED in statuses):
#                 final_status = JobStatus.COMPLETE_WITH_ERRORS
#             elif JobStatus.COMPLETE_EARLY_STOP in statuses:
#                 final_status = JobStatus.COMPLETE_EARLY_STOP
#             else:
#                 final_status = JobStatus.COMPLETE

#             await update_helper(self.parent_job, {"status": final_status})

#         await db_session.commit()
#         return next_iteration