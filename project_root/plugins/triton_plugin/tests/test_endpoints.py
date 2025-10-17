import pytest
import numpy as np 
import httpx 
import asyncio
import os 

EMBEDDING_SIZES = [768, 512, 256, 128, 64, 32]
TEST_ITEMS = [
    "CC(=O)Oc1ccccc1C(=O)O",
    "CN1C=NC2=C1C(=O)N(C(=O)N2C)C",
    "CC12CCC3C(C1CCC2O)CCC4=CC(=O)CCC34C"
]

def test_triton_ping(triton_client):
    response = triton_client.get('/v2/health/live')
    assert response.status_code == 200

async def send_post_request(url, data):
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data)
        return response.json()

def get_payload_keys(request_keys, batch_size):
    bool_list = [True for i in range(batch_size)]
    payload_keys = [
        {
            "name": key,
            "shape": [batch_size, 1],
            "datatype": "BOOL",
            "data": bool_list
        }
        for key in request_keys 
    ]
    return payload_keys 

def create_embedding_request(items, output_size):
    request_keys = [f"compress_{output_size}"]
    payload_keys = get_payload_keys(request_keys, len(items))
    payload_data = [
        {
            "name" :     "sequence",
            "shape" :    [len(items), 1],
            "datatype" : "BYTES",
            "data" :     items
        }
    ] + payload_keys
    payload = {"inputs" : payload_data}
    return payload 

def create_mapper_request(embeddings, output_size):
    input_size = embeddings.shape[-1]
    request_keys = [f"input_size_{input_size}", 
                    f"output_size_{output_size}"]
    payload_keys = get_payload_keys(request_keys, embeddings.shape[0])
    payload_data = [
        {
            "name" :     "embedding",
            "shape" :    list(embeddings.shape),
            "datatype" : "FP32",
            "data" :     embeddings.tolist()
        }
    ] + payload_keys
    payload = {"inputs" : payload_data}
    return payload 


@pytest.mark.parametrize("embedding_size", EMBEDDING_SIZES)
def test_embedding_endpoints(triton_client, embedding_size):
    endpoint = f"/v2/models/EMBED/infer"
    payload = create_embedding_request(TEST_ITEMS, embedding_size)
    
    response = triton_client.post(endpoint, json=payload)
    response.raise_for_status()
        
    response_data = response.json()
    embeddings = np.array(response_data["outputs"][0]["data"])

    assert embeddings.shape[0] == len(TEST_ITEMS) * embedding_size
    _ = embeddings.reshape(len(TEST_ITEMS), embedding_size)
    
    assert not np.any(np.isnan(embeddings))
    assert not np.any(np.isinf(embeddings))


@pytest.mark.parametrize("input_size", EMBEDDING_SIZES)
@pytest.mark.parametrize("output_size", EMBEDDING_SIZES)
def test_mapper_endpoint(triton_client, input_size, output_size):
    endpoint = "/v2/models/DECOMPOSE/infer"
    embeddings = np.random.randn(len(TEST_ITEMS), input_size)
    payload = create_mapper_request(embeddings, output_size)

    response = triton_client.post(endpoint, json=payload)
    response.raise_for_status()
    
    response = response.json()
    raw_output = response["outputs"][0]["data"]
    bs, n_out, d_emb = response['outputs'][0]['shape']
    
    mapper_output = np.array(raw_output)
    assert mapper_output.shape[0] == len(TEST_ITEMS) * output_size * 2
    mapper_output = mapper_output.reshape(len(TEST_ITEMS), 2, -1)
    
    assert not np.any(np.isnan(mapper_output))
    assert not np.any(np.isinf(mapper_output))

    result = []
    for i in range(bs):
        r = []
        for j in range(n_out):
            embedding = raw_output[i * n_out * d_emb + j * d_emb : i * n_out * d_emb + (j + 1) * d_emb]
            r.append(embedding)
        result.append(r)
    result = np.array(result)
    assert (result == mapper_output).all()

@pytest.mark.asyncio
async def test_concurrent_mapper():
    url = f"http://triton_plugin:{os.environ['TRITON_HTTP_PORT']}/v2/models/DECOMPOSE/infer"
    payloads = []
    for input_size in EMBEDDING_SIZES:
        for output_size in EMBEDDING_SIZES:
            embeddings = np.random.randn(len(TEST_ITEMS), input_size)
            payload = create_mapper_request(embeddings, output_size)
            payloads.append(payload)

    tasks = []
    for payload in payloads:
        task = asyncio.create_task(send_post_request(url, payload))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    _ = [i['outputs'][0]['shape'] for i in results]

@pytest.mark.asyncio
async def test_concurrent_embedding():
    url = f"http://triton_plugin:{os.environ['TRITON_HTTP_PORT']}/v2/models/EMBED/infer"
    payloads = [create_embedding_request(TEST_ITEMS, size)
                for size in EMBEDDING_SIZES]
    tasks = []
    for payload in payloads:
        task = asyncio.create_task(send_post_request(url, payload))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    _ = [i['outputs'][0]['shape'] for i in results]
