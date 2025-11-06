# Adding Plugins

VVS discovers and orchestrates algorithms via **plugins**. A plugin is any process that exposes one of the six canonical roles:

* `embedding` – produce vector embeddings for items
* `data_source` – return nearest neighbors (items) for a query embedding
* `filter` – return `{valid: bool}` for an item
* `score` – return `{valid: bool, score: float}` for an item
* `mapper` – transform an embedding into N parent embeddings
* `assembly` – combine N parent items into a product item

Each plugin declares:

* **name**: Human readable plugin name
* **type**: one of the canonical plugin roles above
* **execution_type**: `"api"` (HTTP) or `"queue"` (RabbitMQ)
* **timeout**: maximum request/response timeout in seconds
* **max_concurrency**: maximum number of concurrent outstanding requests to the plugin
* **max_retries**: maximum number of retried in the event of plugin execution failure
* **batch_size**: number of request items in a single request. If `batch_size>1`, requests will be a list of the relevant json request schema. Otherwise, for `batch_size=1`, the request will be json.
* **endpoint_url**: plugin request URL, required for API execution endpoints
* **group_key**: used for message routing for queue execution endpoints
* role-specific fields for different plugin roles
    * `embedding`
        * **vector_length**: length of embedding vector
        * **distance_metric**: distance metric for embedding (one of `Cosine`, `Euclid`, `Dot`)
    * `data_source`, `filter`, `score`
        * **embedding_ids**: list of `embedding` record ids. If provided, all requests made to the plugin will include the relevant embedding vectors. `data_source` plugins require at least one `embedding_id`. 
    * `mapper`
        * **input_embedding_id**: `embedding` record id that will be sent as the input embedding to the mapper
        * **output_order**: defines canonical order of `N` embedding outputs from the mapper
    * `assembly`
        * **num_parents**: number of required inputs to the assembly (must be at least 2)

Once registered, VVS will call the plugin using a strict request/response schema (over HTTP **or** as a RabbitMQ consumer). All request schemas support a `runtime_args` argument, which can be used to pass arbitrary keywords to the plugin at execution time.

```python
class RequestData(BaseModel):
    request_id: Optional[str]
    plugin_id: int 
    plugin_name: str 

class ItemData(BaseModel):
    item_id: int
    external_id: Optional[str]
    item: str 
        
class Embedding(BaseModel):
    plugin_id: int 
    plugin_name: str 
    embedding: List[float]

class ItemDataEmbed(ItemData):
    embedding: Optional[List[Embedding]]=None 
        
class ItemRequest(BaseModel):
    """request schema for filter, score, embedding plugins"""
    request_data: RequestData
    item_data: ItemDataEmbed
    runtime_args: Optional[Dict]=None

class EmbedResponse(BaseModel):
    """embedding plugin response schema"""
    valid: bool 
    embedding: Optional[List[float]]

class DataSourceRequest(BaseModel):
    """data_source request schema"""
    request_data: RequestData
    embedding: Embedding
    k: int 
    runtime_args: Optional[Dict]=None
        
class DataSourceResponseItem(BaseModel):
    model_config = ConfigDict(extra='allow')
    item: str
    external_id: Optional[str]
    embedding: List[float]
    distance: Optional[float]
        
class DataSourceResponse(BaseModel):
    """data_source response schema"""
    valid: bool
    result: Optional[List[DataSourceResponseItem]]
        
class FilterResponse(BaseModel):
    """filter request schema"""
    valid: bool
        
class ScoreResponse(BaseModel):
    """score request schema"""
    valid: bool
    score: Optional[float]
        
class MapperRequest(BaseModel):
    """mapper request schema"""
    request_data: RequestData
    embedding: Embedding
    runtime_args: Optional[Dict]=None
        
class MapperResponse(BaseModel):
    """mapper response schema"""
    valid: bool
    embedding: Optional[List[List[float]]]

class AssemblyItem(ItemData):
    assembly_index: int 

class AssemblyRequest(BaseModel):
    """assembly request schema"""
    request_data: RequestData
    parents: List[AssemblyItem]
    runtime_args: Optional[Dict]=None

class AssemblyResult(BaseModel):
    model_config = ConfigDict(extra='allow')
    item: str 
    external_id: Optional[str]
        
class AssemblyResponse(BaseModel):
    """assembly response schema"""
    valid: bool
    result: Optional[List[AssemblyResult]]
```


Below are quickstarts for Queue and API plugins.

---

## 1) Registering a plugin with the backend

### 1.1 Minimal payloads (HTTP)

**POST `/api/v1/plugins/`** with JSON:

#### Embedding (API)

```json
{
  "name": "my_embed_api",
  "plugin_class": "generic",
  "type": "embedding",
  "execution_type": "api",
  "group_key": "my_group",
  "endpoint_url": "http://my-plugin:8000/embedding",
  "timeout": 10,
  "max_concurrency": 4,
  "max_retries": 2,
  "batch_size": 16,
  "vector_length": 256,
  "distance_metric": "Cosine" // Cosine, Euclid, Dot
}
```

#### Data source (API)

```json
{
  "name": "my_datasource_api",
  "plugin_class": "generic",
  "type": "data_source",
  "execution_type": "api",
  "group_key": "my_group",
  "endpoint_url": "http://my-plugin:8000/data_source",
  "timeout": 10,
  "max_concurrency": 4,
  "max_retries": 2,
  "batch_size": 16,
  "embedding_ids": [123]   // id(s) of embedding plugins this DS accepts
}
```

#### Score (API)

```json
{
  "name": "my_score_api",
  "plugin_class": "generic",
  "type": "score",
  "execution_type": "api",
  "group_key": "my_group",
  "endpoint_url": "http://my-plugin:8000/score",
  "timeout": 10,
  "max_concurrency": 4,
  "max_retries": 2,
  "batch_size": 16,
  "embedding_ids": []   // optional id(s) of embeddings to include with request
}
```

#### Mapper (API)

```json
{
  "name": "my_mapper_api",
  "plugin_class": "generic",
  "type": "mapper",
  "execution_type": "api",
  "group_key": "my_group",
  "endpoint_url": "http://my-plugin:8000/mapper",
  "timeout": 10,
  "max_concurrency": 4,
  "max_retries": 2,
  "batch_size": 8,
  "input_embedding_id": 123,
  "output_order": [ // embedding ids and index order for mapper outputs
    {"index": 0, "embedding_id": 456},
    {"index": 1, "embedding_id": 789}
  ]
}
```

#### Assembly (API)

```json
{
  "name": "my_assembly_api",
  "plugin_class": "generic",
  "type": "assembly",
  "execution_type": "api",
  "group_key": "my_group",
  "endpoint_url": "http://my-plugin:8000/assembly",
  "timeout": 10,
  "max_concurrency": 4,
  "max_retries": 2,
  "batch_size": 8,
  "num_parents": 2
}
```

> For **Queue** plugins, omit `endpoint_url` and use `"execution_type": "queue"`. VVS will publish to RabbitMQ with routing keys like `request.{group_key}.{plugin_type}.*.*.*`.

---

## 2) API Plugin – Minimal server

This is a small FastAPI service that implements **all** six endpoints (you can implement just the ones you need). It accepts either a single object (for `batch_size=1`) or a list (for `batch_size>1`) and returns matching shapes.

```python
from typing import List, Union
from fastapi import FastAPI, HTTPException
import numpy as np
import string

app = FastAPI()


@app.post("/embedding", response_model=Union[schemas.EmbedResponse, List[schemas.EmbedResponse]])
async def embed(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    ...

@app.post("/data_source", response_model=Union[schemas.DataSourceResponse, List[schemas.DataSourceResponse]])
async def data_source(request: Union[schemas.DataSourceRequest, List[schemas.DataSourceRequest]]):
    ...

@app.post("/filter", response_model=Union[schemas.FilterResponse, List[schemas.FilterResponse]])
async def filter(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    ...

@app.post("/score", response_model=Union[schemas.ScoreResponse, List[schemas.ScoreResponse]])
async def score(request: Union[schemas.ItemRequest, List[schemas.ItemRequest]]):
    ...

@app.post("/mapper", response_model=Union[schemas.MapperResponse, List[schemas.MapperResponse]])
async def mapper(request: Union[schemas.MapperRequest, List[schemas.MapperRequest]]):
    ...

@app.post("/assembly", response_model=Union[schemas.AssemblyResponse, List[schemas.AssemblyResponse]])
async def assemble(request: Union[schemas.AssemblyRequest, List[schemas.AssemblyRequest]]):
    ...
```

Run it:

```bash
pip install fastapi uvicorn numpy
uvicorn app:app --host 0.0.0.0 --port 8000
```

Register it with the backend using the JSON payloads above (set `endpoint_url` to `http://<host>:8000/<role>`).

---

## 3) Queue Plugin – Minimal consumer

This is a minimal RabbitMQ consumer that binds to the request routing key pattern and publishes replies to the `reply_to` queue with the same `correlation_id` (RPC style). It shows **one** handler (`score`); replicate for other roles.

```python
# consumer.py
import os, json, pika, numpy as np

# see ./project_root/.env for default rabbitmq arguments
EX = os.environ.get("RABBITMQ_EXCHANGE_NAME", "vvs")
HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
USER = os.environ["RABBITMQ_DEFAULT_USER"]
PASS = os.environ["RABBITMQ_DEFAULT_PASS"]
GROUP = "my_queue"   # must match plugin group_key
EMB_DIM = 32

cred = pika.PlainCredentials(USER, PASS)
params = pika.ConnectionParameters(host=HOST, port=PORT, credentials=cred)

def score(req):
    return {"valid": True, "score": float(10*np.random.rand())}

handlers = {"score": score}  # add 'embedding', 'data_source', etc.

def main():
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    # bind an exclusive, auto-delete queue to all your plugin routing keys
    q = ch.queue_declare(queue="", durable=True, auto_delete=True, arguments={
        "x-dead-letter-exchange": f"{EX}.dlx"
    }).method.queue

    # request.{group_key}.{plugin_type}.*.*.*  (plugin_id, item_id, request_id positions)
    for role in handlers.keys():
        ch.queue_bind(exchange=EX, queue=q, routing_key=f"request.{GROUP}.{role}.*.*.*")

    def on_msg(ch, method, props, body):
        payload = json.loads(body)
        plugin_type = method.routing_key.split(".")[2]  # request.GROUP.<role>
        resp = handlers[plugin_type](payload)

        if props.reply_to:
            ch.basic_publish(
                exchange="",
                routing_key=props.reply_to,
                body=json.dumps(resp),
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id,
                    delivery_mode=2,
                ),
            )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=q, on_message_callback=on_msg)
    print("Queue plugin running…")
    ch.start_consuming()

if __name__ == "__main__":
    main()
```

Register the **Queue** plugin with:

```json
{
  "name": "my_score_queue",
  "plugin_class": "generic",
  "type": "score",
  "execution_type": "queue",
  "group_key": "my_queue",
  "timeout": 5,
  "max_concurrency": 4,
  "max_retries": 1,
  "batch_size": 16
}
```

VVS will publish requests to `request.my_queue.score.*.*.*` and expect the consumer to reply to the `reply_to` queue (RPC), as above.

---

## 4) Minimal client snippets

Register via Python:

```python
import httpx

backend = "http://backend:3000"
payload = { ... }  # any of the JSON payloads shown earlier
r = httpx.post(f"{backend}/api/v1/plugins/", json=payload, timeout=10)
r.raise_for_status()
print("plugin id:", r.json()["id"])
```

Clear plugin cache (optional):

```python
pid = 123
httpx.delete(f"{backend}/api/v1/plugins/clear_cache/{pid}").raise_for_status()
```

---

## 5) Execution Test

After a plugin has been registered, execution can be tested using the `/api/v1/execute/{plugin_id}` endpoint. Send a POST request with the relevant request schema to confirm the plugin is registered and functioning correctly. For requests that require the `item_id` field, use `item_id=-1` for testing purposes.

---

## 6) Troubleshooting

* **Batch size errors**: the platform will send lists when `batch_size>1`; your plugin should accept either single or list payloads and echo the shape back.
* **Timeouts**: set `timeout` and `max_concurrency` realistically; for Queue consumers, ensure `basic_qos(prefetch_count=1)` or tune it per workload.
* **Assembly parents**: `num_parents` dictates how many embeddings/parents `mapper/assembly` must produce/consume.
* **Reply queues (Queue mode)**: always publish the reply to `props.reply_to` with the same `correlation_id`; otherwise requests will hang.

