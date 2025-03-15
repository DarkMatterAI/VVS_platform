# VVS_V2
VVS V2

todos
    build core ops around executioner
        data query
        assembly data query
        mapper data query 
        bb data query
        combined "decomposed data query assembly" 
            mapper can feed into this 
        filter execution
        score execution
        InternalItem data model
        compute embeddings before ops
        add "get required embeddings" to generic execution step?
    jobs
        qdrant upload
        search
            standard
            mapper
            bb
        
should the backend even have an execute endpoint?
    what would we actually use it for?
    all job execution will go direct, ie no backend in the loop
    backend mainly exists for crud 

issues with execution code
    we're currently writing an execution wrapper around the execution wrapper
    should distinguish execute params from set-at-job-time request params
        ie k, runtime args



eventual job todos
    add job id to execution failures
    how to handle initial queries?
        most likely add a `UserItem` table
            item id, job id
            auto delete on item or job delete
    how to determine initial embedding for data source with multiple embeddings?
        create query for each linked embedding?

the embedding thing is annoying 
    concepts
        source embedding
            main embedding used for update and whatnot
    initial queries need to be embedded with source embedding
    item request/mapper request require embeddings
        mapper defined by source embedding
        item (filter/score) may have any number of linked embeddings 


add cache flag to plugins?
    or do at runtime?
runtime args for plugins
    could be used by plugins and job executor

table to track plugin usage?
    just plugin, request id, execute time, number of inputs 

item table
    id, item_id, timestamp

external table
    id (item table), external_id, source plugin id (data or assembly), timestamp

score table
    item_id, score plugin id, score

assembly table
    assembly_id, item_id (product item), assembly plugin id

assembly components table
    assembly_id, item_id (parent item), assembly index


plugin results table
    item_id, plugin_id, valid, score, embedding (json)




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




