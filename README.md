# VVS_V2
VVS V2

todos
    jobs
        qdrant upload
        search
            standard
            mapper
            bb
            


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

