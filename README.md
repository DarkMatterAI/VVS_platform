# VVS_V2
VVS V2

todos
    jobs
        qdrant upload
        search
            standard
            mapper
            bb

database refactor
    do proper execution
        acquire redis lock, etc 


thoughts on item data model
    should assembly have an external id field?
    all saved molecules should have an internal ID unrelated to user external IDs
    an assembled molecule should be linked to parents by parent IDs

    how much item saving do we really want to do?
        scored results of course
        current qdrant data has external id, item, embedding
            move non-embedding to postgres?

        say we want to link assembled molecules to parents by id
            requires all parents to be in postgres with id
            easy if qdrant upload and assembly are the only ways of molecules entering the system
            harder to support user plugin data sources
                ie every data query result will need to be checked into postgres 

        lazy logging
            store assembly parents as a json blob lol

    what is the external id really for?
        comparing results to user dataset?
    when do we need an internal id?

how much logging
    internal datasets
        uploaded datasets
    external datasets
        job inputs
        data source / assembly results


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
    job id, job json, status

results
    result id, job id, item_id, assembly id (optional)

search iteration
    (iteration id, parent iteration id, iteration (int), inference count, time,
    json blob (query embedding, grad, etc), status)
        status to indicate queue, running, fail, complete, etc
        failure notes
            inference/time runout, no valid results, etc

search iteration results
    iteration id, item_id


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


plugin execution flow
    gather unique queries
    create cache keys
        filter/score/embedding
            item id + plugin id
        assembly
            parent ids + plugin id
        data/mapper
            no cache
    hit cache, gather uncached inputs
    check cache miss against database 
        filter/score/embedding
            plugin results table
        assembly
            assembly components table
        data source/mapper
            no database check
    execute plugin
        break into batches
        queue
            post messages such that # of outstanding is never over concurrency
        api
            concurrent post requests gates by concurrency
    check in results
        data source
            item checkin
        score
            score checkin
        assembly
            assembly checkin
        filter/embed/mapper
            no checkin
    scatter to unique queries
    scatter to inputs




deletion concerns
    all items in item table should have at least one external table entry
    deleting a plugin should delete all records linked to that plugin

    data source
        delete plugin
        delete external table refs
        find hanging items, delete
    
    assembly delete
        delete plugin
        delete external table refs
        delete assembly table refs
        find hanging items, delete


search iteration flow
    start with query, gradient
    check into search iteration table
    generate gradient arc queries
    run data queries
    check results into postgres 
    deduplicate results based on id
    check into iteration results table
        do before/after filter and score??
    filter/remove
    score results
    check into score table
    select update
    compute grad
    create next iteration

assembly search iteration flow
    query, grad
    check into iteration table 
    generate grad arc queries
    run data query
        split queries
        check results into postgres
        assemble
        check results into postgres
        embed 

job things to remember
    inference budget
    time limit 


check mapper result contains correct number of embeddings

use new embeddings to test multi-embedding backend 

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
        

plugin execution during job
    generic/before
        create hash keys
        check cache
        gather inputs
        for score
            check inference budget
    api
        acquire lock based on plugin concurrency
        make request 
        release lock
    plugin
        create counter for plugin
        check counter is below concurrency
        post message
        increase counter
        poll result
        get result (or timeout)
        decrease counter 
    generic / after
        save to cache
        scatter 
        for score
            update inference budget (should be per item)
        



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




