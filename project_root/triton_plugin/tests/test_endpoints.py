import pytest
import numpy as np 

EMBEDDING_SIZES = [768, 512, 256, 128, 64, 32]
TEST_ITEMS = [
    "CC(=O)Oc1ccccc1C(=O)O",
    "CN1C=NC2=C1C(=O)N(C(=O)N2C)C",
    "CC12CCC3C(C1CCC2O)CCC4=CC(=O)CCC34C"
]

def test_triton_ping(triton_client):
    response = triton_client.get('/v2/health/live')
    assert response.status_code == 200

def create_embedding_request(items):
    return {
        "inputs": [
            {
                "name": "sequence",
                "shape": [len(items), 1],
                "datatype": "BYTES",
                "data": items
            }
        ]
    }

def create_mapper_request(embeddings):
    return {
        "inputs": [
            {
                "name": "embedding",
                "shape": list(embeddings.shape),
                "datatype": "FP32",
                "data": embeddings.tolist()
            }
        ]
    }


@pytest.mark.parametrize("embedding_size", EMBEDDING_SIZES)
def test_embedding_endpoints(triton_client, embedding_size):
    endpoint = f"/v2/models/EMBED_{embedding_size}/infer"
    payload = create_embedding_request(TEST_ITEMS)
    
    response = triton_client.post(endpoint, json=payload)
    
    assert response.status_code == 200
    
    response_data = response.json()
    embeddings = np.array(response_data["outputs"][0]["data"])

    assert embeddings.shape[0] == len(TEST_ITEMS) * embedding_size
    _ = embeddings.reshape(len(TEST_ITEMS), embedding_size)
    
    assert not np.any(np.isnan(embeddings))
    assert not np.any(np.isinf(embeddings))

def test_mapper_endpoint(triton_client):
    embed_response = triton_client.post(
        "/v2/models/EMBED_64/infer",
        json=create_embedding_request(TEST_ITEMS)
    )
    
    assert embed_response.status_code == 200
    
    embeddings = np.array(embed_response.json()["outputs"][0]["data"])
    embeddings = embeddings.reshape(len(TEST_ITEMS), -1)
    
    # Now test mapper
    mapper_response = triton_client.post(
        "/v2/models/ENAMINE_MAPPER_64/infer",
        json=create_mapper_request(embeddings)
    )
    
    assert mapper_response.status_code == 200
    response = mapper_response.json()
    raw_output = response["outputs"][0]["data"]
    bs, n_out, d_emb = response['outputs'][0]['shape']
    
    mapper_output = np.array(raw_output)
    assert mapper_output.shape[0] == len(TEST_ITEMS) * 64 * 2
    mapper_output = mapper_output.reshape(len(TEST_ITEMS), 2, -1)
    
    # Check data type and values
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

