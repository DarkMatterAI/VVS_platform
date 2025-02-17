# VVS_V2
VVS V2

todos
    rdkit plugin
        remove synt-on??
    general
        add description to plugins?
    add batched requests and batch size to model 
        add tests
            test batch messenger
            test batch message results
            test batch api 
        API batch execute should have concurrency/batch size breakdown
    triton tests 

summary thinking about batched execution
    remember that going through the backend is only required for testing
    batched request only useful for API plugin
    need a batch size to control max items in request
        requires schema change
    all items in batched request should have unique request key
    response should have same number of items 



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
        

