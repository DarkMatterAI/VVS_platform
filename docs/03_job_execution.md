# VVS Job Execution

VVS platform supports three variants of VVS hill climbing jobs:

* **Standard**: Used for searching an enumerated space. Requires a score plugin, data source plugin (containing enumerated outputs), and associated embedding plugin. Optionally supports filter plugins
* **Mapper**: Used for searching building block space. Requires a score plugin, at least one data source plugin (containing building blocks) with associated embedding plugin, assembly plugin and mapper plugin. Optionally supports filter plugins.
* **Assembled**: Used for searching building block space without an assembly plugin. Approximates enumerated molecule embeddings with concatenated building block embeddings to avoid needing a mapper model. Less performant but easier to use out the gate (avoids needing to train a novel mapper model for new datasets). Requires a score plugin, at least one data source plugin (containing building blocks) with associated embedding plugin, and assembly plugin. Optionally supports filter plugins.

---

## Job Concepts at a glance

* **Job inputs** — one or more seed items (or parent pairs for assembled) that the hill-climb iterates from.
* **Update params** — learning rate vectors and distance metric; define how embedding-space moves are computed.
* **Plugin config** — a bundle of 4–6 plugins (score, filters, data sources, embedding(s), and optionally `mapper`/`assembly`).
* **Execute / override params** — runtime controls for how each plugin is called.

### Execute and Override Params

Each plugin used in a job can optionally make use of different parameters to control how the plugin is used at runtime.

**Execute Params** change how the VVS system applies caching and database persistence to plugin execution results. The following parameters are available:

* `cache` (bool, default `False`): cache responses to identical requests. If `True`, plugin results will be cached and the cache will be checked prior to sending an execution request to the plugin.
* `aggressive_cache` (bool, default `False`): If `True`, the system checks the cache for plugin results while a given result is pending. 
* `db_lookup` / `db_persist` (bool, default `False` / `False`): whether to look up/persist responses in DB.
* `use_semaphore` (bool, default `True`): use Redis semaphore to rate-limit calls to the plugin based on the plugin's `concurrency` value.
* `max_semaphore_attempts` (int): how many rounds a request will wait for a semaphore token if `use_semaphore=True`.
* `backoff_factor` (float): exponential backoff multiplier on semaphore waits.
* `log_execute_keys` (bool): include request keys in job logs for debugging.

**Override Params** temporarily override plugin registration knobs for this job **only**:

* `timeout` (seconds)
* `max_concurrency`
* `max_retries`
* `batch_size`
* (API only) `endpoint_url` (rarely used)

> **Best practice:** keep plugin registration conservative, then raise limits via `override_params` for specific jobs/hardware contexts.

---

## Job Execution

### API Execution

Jobs are created via the backend `http://localhost:{NGINX_HTTP_PORT}/api/v1/hc_jobs` endpoint.

Running jobs can be monitored at `http://localhost:{NGINX_HTTP_PORT}/dagster/runs`.

Job outputs can be downloaded from the `http://localhost:{NGINX_HTTP_PORT}/api/v1/hc_jobs/{hc_job_id}/results`endpoint (for flat, paginated results) or the `http://localhost:{NGINX_HTTP_PORT}/api/v1/hc_jobs/{hc_job_id}/export` endpoint for full hierarchical results.

### Python Excution

Jobs can be created and executed from a python environment if the `vvs_database` library is in your path.


---

## 1) Standard job (no mapper, no assembly)

### 1.1 Python (Pydantic)

```python
from vvs_database.schemas.hc_schemas import (
    HCConfigCreate, HCJobCreate, HCJobParams, HCUpdateParams,
    UpdateType, DistanceMetric, HCInputItem
)
from vvs_database.schemas.internal_schemas import (
    ExecutePluginCreate, ExecuteDataSourceCreate, ExecuteDataParams,
    ExecuteParams, PluginOverrideParams
)

# Required plugins
score_config = ExecutePluginCreate(
    plugin_id=score_plugin_id,
    execute_params=ExecuteParams(cache=True),  # enable cache on score requests
)
data_config = ExecuteDataSourceCreate(
    plugin_id=datasource_id,
    data_source_params=ExecuteDataParams(k=10, assembly_index=0),
)
embedding_configs = [ExecutePluginCreate(plugin_id=embedding_id)]

cfg = HCConfigCreate(
    filter_configs=[],                      # optional
    score_config=score_config,
    embedding_configs=embedding_configs,
    data_config=data_config,                # single data source
)

update_params = HCUpdateParams(
    update_type=UpdateType.GLOBAL_UPDATE,
    distance_metric=DistanceMetric.Cosine,
    learning_rate=[1.0, 2.0, 3.0],         # LR vector for single parent path
)

job = HCJobCreate(
    job_params=HCJobParams(auto_execute=False),
    plugin_config=cfg,
    update_params=update_params,
    job_inputs=[
        HCInputItem(item={"external_id": "Z1", "item": "C1=CC=CC=C1"}, max_iterations=5)
    ],
)
```

**Submit** (sync call, returns IDs):

```python
import httpx
r = httpx.post(f"{backend_url}/api/v1/hc_jobs", json=job.model_dump())
r.raise_for_status()
resp = r.json()
print("parent:", resp["parent_job_id"], "inputs:", resp["input_job_ids"])
```

### 1.2 JSON payload

```json
{
  "job_params": {"auto_execute": false},
  "plugin_config": {
    "filter_configs": [],
    "score_config": {"plugin_id": 100, "execute_params": {"cache": true}},
    "embedding_configs": [{"plugin_id": 10}],
    "data_config": {
      "plugin_id": 200,
      "data_source_params": {"k": 10, "assembly_index": 0}
    }
  },
  "update_params": {
    "update_type": "global_update",
    "distance_metric": "Cosine",
    "learning_rate": [1.0, 2.0, 3.0]
  },
  "job_inputs": [
    {"item": {"external_id": "Z1", "item": "C1=CC=CC=C1"}, "max_iterations": 5}
  ]
}
```

---

## 2) Assembled job (N parents via assembly)

### 2.1 Python

```python
from vvs_database.schemas.hc_schemas import (
    HCAssembledConfigCreate, HCAssembledJobCreate, HCAssembledUpdateParams,
    LearningRate, HCAssembedInputItem, HCJobParams, UpdateType, DistanceMetric
)

assembly_config   = ExecutePluginCreate(plugin_id=assembly_plugin_id)
data_configs = [
    ExecuteDataSourceCreate(
        plugin_id=datasource_ids[i],
        data_source_params=ExecuteDataParams(k=7, assembly_index=i),
        # optional DS runtime overrides
    )
    for i in range(num_parents)
    # a list with a single data source can be used to apply the data source
    # to all assembly inputs
]
embedding_configs = [ExecutePluginCreate(plugin_id=embedding_id)]

cfg = HCAssembledConfigCreate(
    filter_configs=[],
    score_config=score_config,
    data_configs=data_configs,
    embedding_configs=embedding_configs,
    assembly_config=assembly_config,
)

# LR per parent path (assembly_index 0..N-1)
lr_cfg = [
    LearningRate(learning_rate=[1.0, 2.0, 3.0], assembly_index=0),
    LearningRate(learning_rate=[1.5, 2.5, 3.5], assembly_index=1),
]

update = HCAssembledUpdateParams(
    update_type=UpdateType.GLOBAL_UPDATE,
    distance_metric=DistanceMetric.Cosine,
    learning_rate=lr_cfg,
)

inputs = HCAssembedInputItem(
    max_iterations=4,
    items=[
        {"external_id": "EN1", "item": "NCC", "assembly_index": 0},
        {"external_id": "EN2", "item": "CCC", "assembly_index": 1}
    ]
)

job = HCAssembledJobCreate(
    job_params=HCJobParams(auto_execute=False),
    plugin_config=cfg,
    update_params=update,
    job_inputs=[inputs],
)
```

### 2.2 JSON payload

```json
{
  "job_params": {"auto_execute": false},
  "plugin_config": {
    "filter_configs": [],
    "score_config": {"plugin_id": 100},
    "embedding_configs": [{"plugin_id": 10}],
    "data_configs": [
      {"plugin_id": 201, "data_source_params": {"k": 7, "assembly_index": 0}},
      {"plugin_id": 202, "data_source_params": {"k": 7, "assembly_index": 1}}
    ],
    "assembly_config": {"plugin_id": 300}
  },
  "update_params": {
    "update_type": "global_update",
    "distance_metric": "Cosine",
    "learning_rate": [
      {"assembly_index": 0, "learning_rate": [1.0, 2.0, 3.0]},
      {"assembly_index": 1, "learning_rate": [1.5, 2.5, 3.5]}
    ]
  },
  "job_inputs": [
    {
      "max_iterations": 4,
      "items": [
        {"external_id": "EN1", "item": "NCC", "assembly_index": 0},
        {"external_id": "EN2", "item": "CCC", "assembly_index": 1}
      ]
    }
  ]
}
```

> **Tip:** If you supply only one data source and an assembly with `num_parents=N`, VVS will **fan-out** the single DS into N copies (assembly indices `0..N-1`).

---

## 3) Mapper job (mapper + assembly + data sources)

### 3.1 Python

```python
from vvs_database.schemas.hc_schemas import (
    HCMapperConfigCreate, HCMapperJobCreate, HCJobParams, HCUpdateParams,
    UpdateType, DistanceMetric, HCInputItem
)

mapper_config    = ExecutePluginCreate(plugin_id=mapper_plugin_id)
assembly_config  = ExecutePluginCreate(plugin_id=assembly_plugin_id)
# output_order[i].embedding_id must be accepted by data source config with assembly_index=i
data_cfgs = [
    ExecuteDataSourceCreate(
        plugin_id=datasource_ids[i],
        data_source_params=ExecuteDataParams(k=5, assembly_index=i)
    )
    for i in range(n_outputs)
    # a list with a single data source can be used to apply the data source
    # to all assembly inputs
]
embedding_cfgs = [ExecutePluginCreate(plugin_id=input_embedding_id)]  # optional: runtime overrides here

cfg = HCMapperConfigCreate(
    filter_configs=[],
    score_config=score_config,
    data_configs=data_cfgs,
    embedding_configs=embedding_cfgs,   # embedding plugins available to mapper/data
    assembly_config=assembly_config,
    mapper_config=mapper_config,
)

update = HCUpdateParams(
    update_type=UpdateType.GLOBAL_UPDATE,
    distance_metric=DistanceMetric.Cosine,
    learning_rate=[0.8, 0.4, 0.2],  # LR vector for the (single) mapper input path
)

job = HCMapperJobCreate(
    job_params=HCJobParams(auto_execute=False),
    plugin_config=cfg,
    update_params=update,
    job_inputs=[HCInputItem(item={"external_id": "ZMAP", "item": "C1=CC=CC=C1"}, max_iterations=4)],
)
```

### 3.2 JSON payload

```json
{
  "job_params": {"auto_execute": false},
  "plugin_config": {
    "filter_configs": [],
    "score_config": {"plugin_id": 100},
    "embedding_configs": [{"plugin_id": 10}],
    "data_configs": [
      {"plugin_id": 210, "data_source_params": {"k": 5, "assembly_index": 0}},
      {"plugin_id": 211, "data_source_params": {"k": 5, "assembly_index": 1}}
    ],
    "assembly_config": {"plugin_id": 300},
    "mapper_config":  {"plugin_id": 400}
  },
  "update_params": {
    "update_type": "global_update",
    "distance_metric": "Cosine",
    "learning_rate": [0.8, 0.4, 0.2]
  },
  "job_inputs": [
    {"item": {"external_id": "ZMAP", "item": "C1=CC=CC=C1"}, "max_iterations": 4}
  ]
}
```

> **Mapper alignment:** `mapper.output_order[i].embedding_id` **must** match the data source embedding for `assembly_index = i`. VVS validates this at job creation.


## 5) Exporting results

### Flat, paginated (scores included)

```bash
curl -G "http://backend:3000/api/v1/hc_jobs/{hc_job_id}/results" \
  --data-urlencode "offset=0" \
  --data-urlencode "limit=1000" \
  --data-urlencode "order_by=score" \
  --data-urlencode "only_valid=true"
```

**Response:**

```json
{
  "success": true,
  "hc_job_id": 123,
  "offset": 0,
  "limit": 1000,
  "order_by": "score",
  "only_valid": true,
  "total": 5321,
  "results": [
    {
      "item": {"item_id": 555, "item": "C1=..."},
      "result_valid": true,
      "score": 0.91,
      "score_valid": true,
      "assembly_id": 42,
      "assembly_components": [
        {"item_id": 101, "item": "NCC", "assembly_index": 0},
        {"item_id": 102, "item": "CCC", "assembly_index": 1}
      ],
      "created_at": "..."
    }
  ]
}
```

### Hierarchical export (input → iterations → results)

```bash
curl "http://backend:3000/api/v1/hc_jobs/{hc_job_id}/export"
```

Returns a nested structure grouping results by input job and iteration.

---

## 6) Tips & pitfalls

* **Assembled LR vectors**: supply a `LearningRate` entry for every parent path (`assembly_index: 0..N-1`), no gaps.
* **Data source fan-out**: the number of `data_configs` must either be exactly 1 or match the number of inputs to the assembly.
* **Data source k**: Use a large `k` for search in enumerated spaces and a small `k` for search in building block spaces (assume roughly `k^2` results).
* **Auto-execute**: if `job_params.auto_execute=true`, your orchestrator should pick up and run inputs immediately. Otherwise, kick jobs by calling your runner or queue.
* **Export filters**: add `only_valid=true` when you want to ignore invalid results (e.g., filtered out or assembly failures).
