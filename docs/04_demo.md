# Demo

This demo walks through a building block space search using a toy dataset of 100 Enamine building blocks.

## Setup

### VVS Setup

For this demo, start VVS with the following plugins enabled:

* Triton Plugin
* Qdrant Plugin
* RDKit Plugin

This demo uses the default `NGINX_HTTP_PORT=3000`.

If running on a system with multiple CPUs, consider scaling the RDKit plugin:

```
docker compose scale rdkit_plugin={N_REPLICAS}
```

This has little impact on performance for this demo, but makes a significant difference when running on a full-scale dataset.


### Local Setup

This demo uses the VVS Python SDK. Ensure the `vvs_database` library is in your local path.

### Files Used

This demo uses the files found in `VVS_platform/docs/demo_files`.

## Building Block Search

### Setup: Pull Plugin Information

To run a search in building block space, we need four plugins:

* **Score plugin** — the objective to optimize. We use the stock QED score included with the RDKit plugin.
* **Assembly plugin** — executes SMARTS assembly. We use the stock set of Enamine assembly reactions included with the RDKit plugin.
* **Mapper plugin** — projects embeddings into building block space. We use the stock 256-dim mapper included with the Triton plugin (designed for two-building-block assemblies).
* **Data source plugin** — the vector space to search. We create this later.

All plugins except the data source already exist. We can pull their information using the backend API.

```python
import httpx
from vvs_database.schemas import PluginType

# Backend URL accessible from local environment
backend_url = "http://localhost:3000"

# Filters for locating relevant plugins
plugin_filter_config = {
    'mapper_plugin' : {'type' : PluginType.MAPPER, 'name' : '%Triton Mapper 256->256%'},
    'assembly_plugin' : {'type' : PluginType.ASSEMBLY, 'name' : '%Enamine Reaction All%'},
    'score_plugin' : {'type' : PluginType.SCORE, 'name' : '%QED Score%'}
}
plugin_config = {}

for key, filter_params in plugin_filter_config.items():
    # Query backend for plugin
    plugins = httpx.get(f"{backend_url}/api/v1/plugins/", params=filter_params)
    assert plugins.status_code==200
    plugins = plugins.json()
    
    # Pull plugin id
    plugin = plugins[0]
    plugin_config[key] = plugins[0]["id"]
        
    # For decomposer, pull relevant embeddings
    if key=='mapper_plugin':
        plugin_config['embedding_plugin'] = plugin["output_order"][0]["embedding_id"]
        
print(plugin_config)
```

### Setup: Qdrant Data Source

Next, we create the vector database to search. This involves three steps: (1) upload a CSV of building blocks, (2) create a data source record on the backend, and (3) create a Qdrant upload job.

```python
# 1) upload building block csv to backend
bb_path = "demo_files/demo_bbs.csv" 
upload_url = f"{backend_url}/api/v1/files/upload"

with open(bb_path, 'rb') as f:
    files = {'file': (bb_path.split('/')[-1], f)}

    # Upload to backend via POST request
    response = httpx.post(upload_url, files=files)
    assert response.status_code==200
    
# 2) create data source record
data_source_create = {
    "name" : "demo_bbs",
    "qdrant_config": {
        "vectors_config": [
            {
                "embedding_id": plugin_config['embedding_plugin'], # embedding must be compatible with mapper model
                "hnsw_config": {
                    "m": 32,
                    "ef_construct": 400,
                    "on_disk": False,
                },
                "on_disk": False,
                "datatype": "float16"
            }
        ]
    }
}

response = httpx.post(f"{backend_url}/api/v1/qdrant_plugins/", json=data_source_create)
assert response.status_code==200
response = response.json()
data_id = response['id']
plugin_config['data_plugin'] = data_id # add data source ID to plugin_config

# 3) create qdrant upload job
upload = {
    'plugin_id' : data_id,
    'filename' : bb_path.split('/')[-1],
    'items' : None,
    'save_snapshot' : False,
    'embedding_configs' : None
}

response = httpx.post(f"{backend_url}/api/v1/qdrant_plugins/create_upload_job",
                      json=upload, params={'auto_execute' : True})
assert response.status_code==200
```

Once executed, a Qdrant upload job is triggered on Dagster and can be viewed at `http://localhost:3000/dagster/runs`. Note that the job may take a minute or so to initiate.

The upload job reads the CSV, embeds each building block, and constructs the vector database. After the job finishes, we can verify the embeddings were added:

```python
response = httpx.get(f"{backend_url}/api/v1/plugins/{data_id}")
assert response.status_code==200
response = response.json()
print(response["config"]["collection_info"]["points_count"])
```

To run this with a different set of building blocks, upload a new CSV that follows the same column schema as `demo_bbs.csv`.


### Create Job Config

Next, we create the job config. This involves reading SMILES from `demo_smiles.csv` and setting execution parameters for each plugin. See the Job Executions documentation for an overview of the available execution parameters.

```python
import csv
import numpy as np
from vvs_database.schemas import (
    HCUpdateParams,
    ExecutePluginCreate,
    ExecuteDataSourceCreate,
    PluginOverrideParams,
    ExecuteParams,
    UpdateType,
    DistanceMetric,
    ExecuteDataParams,
    HCMapperConfigCreate,
    HCJobParams,
    HCInputItem,
    HCMapperJobCreate
)

# Configure job parameters 
iterations = 2
k = 7
lrs = (np.array([1,2,3,4,5,6])*1000).tolist() # See paper for explanation of learning rate magnitude 

# update parameters
update_params = HCUpdateParams(update_type=UpdateType.GLOBAL_UPDATE,
                               distance_metric=DistanceMetric.Cosine,
                               learning_rate=lrs)

# score parameters
score_config = ExecutePluginCreate(plugin_id=plugin_config['score_plugin'],
                                   # Optional: override concurrency
                                   override_params=PluginOverrideParams(max_concurrency=1024,
                                                                        timeout=120),
                                   # Optional: cache score results for efficiency 
                                   execute_params=ExecuteParams(cache=True, 
                                                                aggressive_cache=True,
                                                                max_semaphore_attempts=30))

# data source parameters
data_configs = [ExecuteDataSourceCreate(plugin_id=plugin_config['data_plugin'],
                                        data_source_params=ExecuteDataParams(k=k, assembly_index=i),
                                        runtime_args={"params": {"exact": False}}) 
                for i in range(2)]

# embedding parameters
embedding_configs = [ExecutePluginCreate(plugin_id=plugin_config['embedding_plugin'],
                                         override_params=PluginOverrideParams(max_concurrency=256))]

# assembly parameters
assembly_config = ExecutePluginCreate(plugin_id=plugin_config['assembly_plugin'],
                                      override_params=PluginOverrideParams(max_concurrency=1024))

# mapper parameters
mapper_plugin_config = ExecutePluginCreate(plugin_id=plugin_config['mapper_plugin'])

# job config 
mapper_config = HCMapperConfigCreate(filter_configs=[],
                                     score_config=score_config,
                                     data_configs=data_configs,
                                     embedding_configs=embedding_configs,
                                     assembly_config=assembly_config,
                                     mapper_config=mapper_plugin_config)

# Load demo query smiles
smiles_path = "demo_files/demo_smiles.csv"

with open(smiles_path, 'r') as file:
    reader = csv.reader(file)
    header = next(reader)
    data = list(reader)
    
smiles = [{"external_id": i[0], "item": i[1]} for i in data]

# Convert SMILES to input items 
input_items = [HCInputItem(item=i, max_iterations=iterations) for i in smiles]
```

### Execute Job on Backend

We can now send the job config to the backend for execution.

```python
inference_budget = 10000

job_params = HCJobParams(auto_execute=True, inference_limit=inference_budget)

# Job create json
mapper_create = HCMapperJobCreate(job_params=job_params,
                              plugin_config=mapper_config,
                              update_params=update_params,
                              job_inputs=input_items)


# Create job on backend
mapper_job = httpx.post(f"{backend_url}/api/v1/hc_jobs", json=mapper_create.model_dump())

assert mapper_job.status_code == 200
job = mapper_job.json()
assert job["success"] == True
job_id = job["parent_job_id"]
```

This triggers a Dagster run that can be viewed at `http://localhost:3000/dagster/runs` (it may take a minute to start). Once the job is running, we can wait for it to complete and pull down the results.

```python
from vvs_database.schemas import TERMINAL_STATUSES
import time

print("Waiting on job")
while True:
    job = httpx.get(f"{backend_url}/api/v1/jobs/{job_id}")
    job = job.json()
    if job["status"] in TERMINAL_STATUSES:
        print("Job Complete")
        break
    time.sleep(1)

all_results = []
offset = 0
limit = 5000
total = None

while (total is None) or (offset < total):
    response = httpx.get(f"{backend_url}/api/v1/hc_jobs/{job_id}/results",
                         params={"only_valid": True, "offset": offset, "limit": limit})
    response = response.json()

    if total is None:
        total = response["total"]

    all_results += response["results"]
    offset += limit
    
print(all_results[0])
```

## Iterative Search

When searching larger spaces, it is often advantageous to run multiple search iterations, each combining fresh SMILES queries with the highest-scoring molecules from previous iterations. This explore–exploit strategy (see Section 4.9.3 of the paper) balances exploration of new regions of chemical space with refinement of promising hits. The following code demonstrates this approach.

```python
all_jobs = []
all_results = []
last_results = None

n_inputs = 24 # number of inputs per job
total_inference = 0
percent_explore = 0.6
fresh_queries = ... # larger pool of query SMILES, each as {"external_id": ..., "item": <smiles>}

inference_budget = 50000 # maximum inference budget across all jobs
while total_inference < inference_budget:
    if not fresh_queries:
        # exhausted new queries 
        break
    
    job_params = HCJobParams(auto_execute=True, 
                             inference_limit=inference_budget-total_inference)

    if last_results and percent_explore>0:
        exploit_size = int(n_inputs * (1 - percent_explore))
        explore_size = n_inputs - exploit_size
        top_results = [{"external_id": None, "item": i["item"]["item"]} 
                       for i in last_results[:exploit_size]] # note `last_results` is returned sorted by score
        new_queries = fresh_queries[:explore_size]
        fresh_queries = fresh_queries[explore_size:]
        input_items = top_results + new_queries
    else:
        # first iteration, no exploit queries 
        input_items = fresh_queries[:n_inputs]
        fresh_queries = fresh_queries[n_inputs:]

    input_items = [HCInputItem(item=i, max_iterations=iterations) for i in input_items]

    mapper_create = HCMapperJobCreate(job_params=job_params,
                                  plugin_config=mapper_config,
                                  update_params=update_params,
                                  job_inputs=input_items)
    
    print("Creating Job")
    mapper_job = httpx.post(f"{backend_url}/api/v1/hc_jobs", json=mapper_create.model_dump())

    assert mapper_job.status_code == 200
    job = mapper_job.json()
    assert job["success"] == True
    job_id = job["parent_job_id"]
    all_jobs.append(job_id)

    print("Waiting on job")
    while True:
        job = httpx.get(f"{backend_url}/api/v1/jobs/{job_id}")
        job = job.json()
        if job["status"] in TERMINAL_STATUSES:
            print("Job Complete")
            break
        time.sleep(1)

    print("Job complete, gathering results")
    offset = 0
    limit = 5000
    total = None
    last_results = []

    while (total is None) or (offset < total):
        response = httpx.get(f"{backend_url}/api/v1/hc_jobs/{job_id}/results",
                             params={"offset": offset, "limit": limit})
        response = response.json()

        if total is None:
            total = response["total"]
            total_inference += total

        last_results += response["results"]
        offset += limit
        
    all_results += last_results

    print(f"Completed {len(all_jobs)} jobs, {total_inference} total inference")
```