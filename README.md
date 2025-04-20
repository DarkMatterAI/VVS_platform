# VVS_V2
VVS V2

todos
    "clear plugin records" api to delete associated item records without affecting things
    verbosity flag to executor 
    HC update api endpoint 
    dagster
        think more about concurrent qdrant uploads
            acquire semaphore at start of job, release at end
            requires above cancellation/failure handling to release on fail/cancel 
    jobs
        search
            standard
            mapper
            bb
    test gaps
        anything related to dagster 
        qdrant failure uploads 
        unit tests on ops 

setting up HC data models
    figure out crud and job json format
    make sure unique constraints are working 



search job control flow
    start iteration
        check record
        if status is complete_early_stop, early exit
        run iteration 
    at end of iteration
        update iteration record
        update input/job parent records
        check inference and timeout 
    create next iteration
        if pass, create with status queued
        else, create with status complete_early_stop


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


