# VVS_V2

todos
    message refactor
        replace message consumer with direct reply via correlation id 
        correctly process batches 
    performance
        refactor HCJob to batch gather/scatter all HCIterationJobs 
        speed up delete job speed for large record counts
    data sources
        "in memory" datasource for user uploaded embeddings
            add new PluginExecutionType
            update plugin crud logic to only allow for data source to use that
            config points to embedding file that has already been uploaded 
            use safetensors format
        makes more sense after we refactor HCJob to run in batch 
    backend
        HC jobs endpoints
        export HC results endpoint
        BBKNN endpoints 
        move rdkit/qdrant code to db lib 
    HC job
        refactor
            do job level, not input level
            maybe have local cache to lower redis hits?
        dagster sensor
            needs to handle job hierarchy
        dagster failure sensor
            needs to handle job hierarchy, remove all semaphores
        results tracking
            ability to dump iteration state into json and save to postgres
            remove HCIterationResult
            final iteration should create a "next iteration" to save next query/grad
            add execution time tracking for different steps 
        data export
            backend api for data export
            be able to export data by HCJob or HCInputJob 
        input format
            input pre-computed embeddings in addition to items 
        optional args
            k expansion on first query?
        misc
            check what happens when one input job gets 0 results (ie filter failure )
            possibly change mapper response schema to explicitly state assembly index
    qdrant job
        need to control concurrent jobs
            semaphore or dagster limit 
        toggle indexing with `m` instead of indexing threshold (https://qdrant.tech/articles/indexing-optimization/)
        update qdrant formatter to use extra args
    rdkit plugin
        deduplicate reaction list
    misc 
        version pin on all requirements.txt
        better logs/less spam on executor
        qdrant collections after proper postgres persist 
        "clear plugin records" function to remove items/etc
            items with assembly won't auto-clear even after job/plugin deletion 
        fix job cleanup
            currently pulling any HC-related job with no plugin reference
    update plugin type
        new plugin type for custom update
    file organize
        move test modules into single folder
        move plugins into single folder 
    test gaps
        anything on dagster
        qdrant upload failures 
    hanging design decisions
        vector database with multiple embeddings
            does it work for HC/qdrant upload?
        HC update
            does it work to return multiple update outputs? ie fan out
        crud
            check plugin exeuction failures during jobs are being accurately tracked


thinking about bbknn job
    ideally would work for bbknn + lr sweep
    would re-use HCSearchConfigs and whatnot (subclass to make score/update optional)
        if score/update provided, compute scores and gradient 
    do we want to add update params to individual items for HC?
    do we want to add query group tracking to HC results?
    line of thinking:
        for bbknn, want to be able to do lr sweep as well
            requires doing query, scoring results, computing gradient, doing grad query
            analysis requires knowing query groups and embeddings

hc thoughts
    would be nice to have different update params on different inputs
    would be nice to have embedding/gradient inputs
        make `HCInputEmbedding` similar to `HCInputItems`
            job_id, plugin_id (for embedding), assembly_index, embedding (jsonb, optionally with gradient)
    okay lets think about the param thing though
        makes the schema busier
        user could just create another job 

add "next gradient" to hciteration job - allows for starting up after stop 
    create HCIteration job with status complete just to store

optional flag "compute query gradient"
    save each post-grad expasion query with gradient calculated from query results (ie in query group)



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