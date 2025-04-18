import numpy as np 
from vvs_database.schemas.enums import DistanceMetric

def euclid_gradient(query_embedding: np.ndarray, # shape (1, d)
                    embeddings: np.ndarray, # shape (n, d)
                    advantages: np.ndarray, # shape (n, )
                   ) -> np.ndarray:
    """Estimates the gradient at `query_embedding` using 
    `embeddings` and `advantages` using euclidean distance.
    
    The gradient is computed directly using numpy. This 
    is the equivalent gradient to computing the following in pytorch
    
    ```
    # assume `query_embedding` has `requires_grad=True`
    distance = (query_embedding - embeddings).pow(2).mean(-1)
    loss = (advantages * distance).mean()
    loss.backward()
    ```
    """
    grad = (2*advantages[:,None] * (query_embedding - embeddings)).mean(0) / query_embedding.shape[1]
    return grad

def cosine_gradient(query_embedding: np.ndarray, # shape (1, d)
                    embeddings: np.ndarray, # shape (n, d)
                    advantages: np.ndarray, # shape (n, )
                   ) -> np.ndarray:
    """Estimates the gradient at `query_embedding` using 
    `embeddings` and `advantages` using cosine distance.
    
    The gradient is computed directly using numpy. This 
    is the equivalent gradient to computing the following in pytorch
    
    ```
    # assume `query_embedding` has `requires_grad=True`
    distance = 1 - torch.cosine_similarity(query_embedding, embeddings, dim=1)
    loss = (advantages * distance).mean()
    loss.backward()
    ```
    """
    query_norm = np.linalg.norm(query_embedding, axis=-1, ord=2)
    embedding_norms = np.linalg.norm(embeddings, axis=-1, ord=2)
    denom = query_norm * embedding_norms
    dot_product = (embeddings * query_embedding).sum(-1)
    adv_denom = advantages / denom
    
    term1 = (embeddings * adv_denom[:,None]).mean(0)
    term2 = (dot_product * adv_denom).mean() * (query_embedding[0] / query_norm**2)
    grad = term2 - term1
    return grad

def dot_gradient(query_embedding: np.ndarray, # shape (1, d)
                 embeddings: np.ndarray, # shape (n, d)
                 advantages: np.ndarray, # shape (n, )
                ) -> np.ndarray:
    """Estimates the gradient at `query_embedding` using 
    `embeddings` and `advantages` using dot product distance.
    
    The gradient is computed directly using numpy. This 
    is the equivalent gradient to computing the following in pytorch
    
    ```
    # assume `query_embedding` has `requires_grad=True`
    distance = (embeddings * query_embedding).sum(-1)
    loss = (advantages * distance).mean()
    loss.backward()
    ```
    """
    grad = (advantages[:,None] * embeddings).mean(0)
    return grad

distance_to_update = {
    DistanceMetric.Cosine : cosine_gradient,
    DistanceMetric.Euclid : euclid_gradient,
    DistanceMetric.Dot : dot_gradient
}