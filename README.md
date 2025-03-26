# VVS_V2
VVS V2

todos
    dagster
        io managers, local vs prod 
        concurrency
    logging
        replace print with proper logger
        add timestamps, etc
        clean up excessive logging in executor 
        for redis cache, distinguish cache check from message response check
        will need unified logging id for multiple ops
        probably need job id on requests?
        integrate with dagster 
    jobs
        qdrant upload
        search
            standard
            mapper
            bb




eventual job todos
    add job id to execution failures
    how to handle initial queries?
        most likely add a `UserItem` table
            item id, job id
            auto delete on item or job delete
    how to determine initial embedding for data source with multiple embeddings?
        create query for each linked embedding?


jobs table
    job id, job type, job json, status

results
    result id, job id, item_id, assembly id (optional)
        score? can find with query but could be easier to just store again





search iteration
    iteration id - id
    seed iteration id - first iteration 
    parent iteration id - previous iteration
    iteration - int iteration count
    inference count - int inference so far
    time - int time elapsed
    iteration blob - json blob with query, grad, etc
    status - str queue, running, fail, complete, etc
    failure notes - str, any details on failure 

search iteration results
    iteration id, item_id, valid (item made it past all filters to score)


search iteration
    init - embedding, gradient
    do data queries
        check in results
        store to search iteration results table
    filter
    score
        check in results
    update
    compute grad 

assembly search iteration
    init - embedding, gradient
    do data queries
        de-concat
        data query
        check in results
    do assembly
        check in results
        store to search iteration results table
    filter
    score
        check in results
    update
    compute grad


locks/control of plugins/jobs
    inference budget
    time limit
    concurrency         


search iteration flow
    check time/inference limit
    inputs
        query embedding
        gradient embedding
        job id / info
    data source
        generate gradient arc queries
        standard
            data query with checkin
        mapper
            mapper
            split data query with checkin
            generate assembly pools
            assembly with checkin
        bb
            deconcat
            split data query with checkin
            generate assembly pools
            assembly with checkin
        parse result into items format
    check inference limit
    filter
        for each filter
            compute embedding(s) as needed
            execute filter (checkin optional)
            subset for valid results
    score
        compute embedding(s) as needed
        execute score with checkin
    update
        select best result
        compute gradient around result 
    save search iteration
    check time/inference limit 
    create next iteration if applicable 


search crud
    name
    plugins
        data plugins
            data source
            mapper/data sources/assembly
            data sources/assembly
        filters
        score
    update
        topk
        lrs
        etc 
    job params
        filename
        delete file
        time
            time limit
            global time limit
        inference
            inference budget
            global inference budget 
    
all plugin params
    persist, cache, semaphore
    runtime args 

data params
    k





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

search job data model
    data source
        id, k, runtime args
    query embeddings
        id, from data source
    filters
        id, runtime args
    filter embeddings
        id, from filters
    scores
        id, runtime args
    score embeddings
        id, from scores
    update 
        params
    initial queries
        json 

standard data
    data source

mapper data
    mapper
    parent data sources (one per parent)
    assembly

bb data
    parent data sources
    assembly

standard
    initialize
        embed queries
        no gradient
    iteration
        (embed, grad) pair


saved data
    results
        item id, item, job id, filter results, score results
    search iteration
        query/gradient
        num results, other metadata
        result ids
            link back to results table
        parent id 
        job id 

iteration approach
    dagster job is a single iteration
        query
            map/assemble as needed
        filter/score
        update
        grad
    stash on redis after job
    next job reads from redis 
    integrates with having a database for iterations 
    one issue - need way of generating consistent IDs for assembled molecules 




# async def qdrant_query(db_record, request):
#     async with get_qdrant_client() as client:
#         collection_name = f"data_source_{db_record.id}"
#         embedding_name = f"embedding_{request['id']}"
#         qdrant_results = await client.query_points(
#             collection_name=collection_name,
#             query=request['embedding'],
#             using=embedding_name,
#             limit=request['k'],
#             with_vectors=True
#         )

#         results = [] 
#         for result in qdrant_results.points:
#             result_data = {
#                 'external_id' : result.payload.get('external_id', 0),
#                 'item' : result.payload.get('item', ''),
#                 'embedding' : result.vector[embedding_name],
#                 'distance' : result.score
#             }
#             results.append(result_data)
#         return results

