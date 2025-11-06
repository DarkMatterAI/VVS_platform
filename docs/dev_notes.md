# Dev notes

todos
    bbknn endpoint
        think about auto-clean up of assembly records in job delete
    documentation 
    persist postgres
    export vvs hierarchy 

    known bugs
        rdkit plugins break with batch size override 
    performance
        speed up delete job speed for large record counts
    HC job
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
    test gaps
        dagster
        qdrant upload failures 
    hanging design decisions
        vector database with multiple embeddings
            does it work for HC/qdrant upload?
        HC update
            does it work to return multiple update outputs? ie fan out
        crud
            check plugin exeuction failures during jobs are being accurately tracked


documentation
    setup / teardown 
    adding plugins
        queue
        api
    VVS job
        via python
        via json
        parameters
    plugins
        rdkit
            plug backend crud
        qdrant
            plus backend crud 
        triton
        tei
    system design