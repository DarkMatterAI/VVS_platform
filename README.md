# VVS_V2
VVS V2

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
        dagster implementation
        dagster fail sensor probably needs something for HC job hierarchy 
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

