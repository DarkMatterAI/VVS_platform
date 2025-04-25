# VVS_V2

todos
    misc
        "clear plugin records" api to delete associated item records without affecting things
            HC job with assembly results in item records with assembly. these records persist after 
                job deletion due to the assembly
        verbosity flag to executor
            generally reduce log spam 
        update plugin endpoint
            allow custom update for HC job
            need plugin type, plugin crud, api schema
    qdrant job
        think more about running concurrent jobs. maybe restrict to one?
        could use existing semaphore for qdrant, would need way of releasing on job failure 
        idea
            do embed and upload in same step? worse for reproduciblity (lose checkpoint between embed and upload)
            allows for trading off embed concurrency and upload concurrency (less embed semaphore pressure)
            embed seems to be the big time sink anyway 
            food for thought 
    HC job
        dagster sensor
            needs to handle job hierarchy
            needs to release semaphores from all sub-jobs 
        add "n_results" column in addition to inference
        k expansion on first iteration 
        way of tracking inference vs db/cache lookup for scores 
    backend
        endpoints for creating HC jobs
        bbknn endpoint 
    refactor
        replace message consumer with direct reply (correlation id)
        replace plugin integration server
            special api subclass for tritiom/TEI
            special executor for qdrant
        "semaphore group" on plugin 
            could also use plugin type (internal flags, etc)
        deduplicate rdkit reactions
    test gaps
        anything on dagster
        qdrant upload failures 



documentation notes
    backend
    message consumer
        role of main/dlx/alt queue
    plugin integration
    plugins
        rdkit
            reactions assume no reagents 
        tei
        triton 



<!-- 
async def fetch_hc_job_results(
    db,
    hc_job_id: int,
    *,
    offset: int = 0,
    limit: Optional[int] = 100,
    order_by: Literal["score", "timestamp"] = "score",
    only_valid: bool = False,              # ✨ new flag
) -> List[Dict[str, Any]]:
    """
    Fetch HCResult rows for one HC job.

    If *only_valid* is True, return only rows whose `result_valid` flag is True.
    The rest of the API (ordering, pagination) is unchanged.
    """

    # ── 1) find the score‑plugin ───────────────────────────────────────────
    hc_job: HCJob = await db.get(HCJob, hc_job_id)

    try:
        score_plugin_id = hc_job.job_json["plugin_config"]["score_config"]["plugin_id"]
    except (TypeError, KeyError):
        score_plugin_id = (
            await db.execute(
                select(JobPlugin.plugin_id)
                .join(Plugin, Plugin.id == JobPlugin.plugin_id)
                .where(JobPlugin.job_id == hc_job_id, Plugin.type == "score")
            )
        ).scalar_one_or_none()

    if score_plugin_id is None:
        raise ValueError(f"Cannot determine score‑plugin for job {hc_job_id}")

    # ── 2) build the base select ───────────────────────────────────────────
    stmt = (
        select(HCResult)
        .options(
            joinedload(HCResult.item),
            joinedload(HCResult.assembly)
            .selectinload(Assembly.components)
            .joinedload(AssemblyComponent.component),
        )
        .outerjoin(
            ItemResult,
            and_(
                ItemResult.item_id == HCResult.item_id,
                ItemResult.plugin_id == score_plugin_id,
            ),
        )
        .add_columns(
            ItemResult.score.label("item_score"),
            ItemResult.valid.label("score_valid"),
        )
        .where(HCResult.job_id == hc_job_id)
        .offset(offset)
    )

    # extra filter when caller only wants valid results
    if only_valid:
        stmt = stmt.where(HCResult.valid.is_(True))

    # ordering / limit
    if order_by == "score":
        stmt = stmt.order_by(nulls_last(desc("item_score")))
    elif order_by == "timestamp":
        stmt = stmt.order_by(asc(HCResult.created_at))
    else:  # pragma: no cover – Literal guards, but be defensive
        raise ValueError("order_by must be 'score' or 'timestamp'")

    if limit is not None:
        stmt = stmt.limit(limit)

    rows = await db.execute(stmt)

    # ── 3) format result list ──────────────────────────────────────────────
    out: List[Dict[str, Any]] = []
    for hc_res, score, score_valid in rows:
        comp_items: List[Dict[str, Any]] = []
        if hc_res.assembly:
            for comp in sorted(hc_res.assembly.components, key=lambda c: c.assembly_index):
                comp_items.append(
                    {
                        "item_id": comp.component_id,
                        "item": comp.component.item,
                        "assembly_index": comp.assembly_index,
                    }
                )

        out.append(
            {
                "item": {"item_id": hc_res.item_id, "item": hc_res.item.item},
                "result_valid": hc_res.valid,
                "score": score,
                "score_valid": score_valid,
                "assembly_id": hc_res.assembly_id,
                "assembly_components": comp_items,
                "created_at": hc_res.created_at,
            }
        )

    return out -->