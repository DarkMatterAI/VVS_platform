# VVS_V2

todos
    create HC job endpoint
    export hc job results endpoint 
    bbknn endpoint
    documentation 

    known bugs
        job cleanup removes HC iteration jobs because they have no plugin references
        rdkit plugins break with batch size override 
    performance
        speed up delete job speed for large record counts
    backend
        HC jobs endpoints
        export HC results endpoint
        BBKNN endpoints (or just api)
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

