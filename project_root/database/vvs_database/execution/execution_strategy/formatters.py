# vvs_database.execution.execution_strategy.formatters.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union
import numpy as np 
import httpx 

from vvs_database.schemas import PluginInDB, PluginClass, PluginType

class BaseFormatter(ABC):
    """Translate between VVS request / response objects and a remote REST schema."""
    def __init__(self, plugin: PluginInDB):
        self.plugin = plugin 
    # ---------- request side ---------- #
    @abstractmethod
    def build_payload(self, 
                      batch: List[Dict],
                      batch_size: int 
                      ) -> Union[Dict, List[Dict]]:
        ...

    # ---------- response side ---------- #
    @abstractmethod
    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        ...

# ---------------------------------------------------------------------------
# 1. generic – already matches VVS schema
# ---------------------------------------------------------------------------
class GenericFormatter(BaseFormatter):
    def build_payload(self, 
                      batch: List[Dict],
                      batch_size: int 
                      ) -> Union[Dict, List[Dict]]:
        payloads = [r["request"].model_dump() for r in batch]
        if batch_size == 1:
            payloads = payloads[0]
        return payloads 

    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        if batch_size == 1:
            http_resp = [http_resp]
        return http_resp 

# ---------------------------------------------------------------------------
# 2. TEI internal model -----------------------------------------------------
# ---------------------------------------------------------------------------
class TEIFormatter(BaseFormatter):
    def build_payload(self, 
                      batch: List[Dict],
                      batch_size: int 
                      ) -> Union[Dict, List[Dict]]:
        payload = {"inputs": [i["request"].item_data.item for i in batch]}
        payload.update(self.plugin.config)
        print(payload)
        return payload 
    
    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        http_resp = [{"embedding": i, "valid": True} for i in http_resp]
        return http_resp 

# ---------------------------------------------------------------------------
# 3. Triton embedding / mapper ----------------------------------------------
# ---------------------------------------------------------------------------
class TritonMapperFormatter(BaseFormatter):
    def build_payload(self, 
                      batch: List[Dict],
                      batch_size: int 
                      ) -> Union[Dict, List[Dict]]:
        emb = [r["request"].embedding.embedding for r in batch]
        payload = {
            "inputs": [
                {
                    "name":     "embedding",
                    "shape":    [len(batch), len(emb[0])],
                    "datatype": "FP32",
                    "data":     emb,
                }
            ]
        }
        return payload 

    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        response_data = http_resp["outputs"][0]
        data = response_data["data"]
        bs, n_out, d_emb = response_data["shape"]

        result = []
        for i in range(bs):
            r = {"valid": True, "embedding": []}

            for j in range(n_out):
                embedding = data[i * n_out * d_emb + j * d_emb : i * n_out * d_emb + (j + 1) * d_emb]
                r["embedding"].append(embedding)
            result.append(r)
        return result 
    
class TritonEmbeddingFormatter(BaseFormatter):
    def build_payload(self, 
                      batch: List[Dict],
                      batch_size: int 
                      ) -> Union[Dict, List[Dict]]:
        payload = {
            "inputs" : [
                {
                    "name" :     "sequence",
                    "shape" :    [len(batch), 1],
                    "datatype" : "BYTES",
                    "data" :     [i["request"].item_data.item for i in batch]
                }
            ]
        }
        return payload 

    
    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        response_data = http_resp["outputs"][0]
        data = response_data["data"]
        n_out, d_out = response_data["shape"]
        result = output = [{"embedding": data[i*d_out:(i+1)*d_out], "valid": True} for i in range(n_out)]
        return result 
    
# ---------------------------------------------------------------------------
# 4. Qdrant -----------------------------------------------------------------
# ---------------------------------------------------------------------------

class QdrantFormatter(BaseFormatter):
    def build_payload(self, 
                      batch: List[Dict],
                      batch_size: int 
                      ) -> Union[Dict, List[Dict]]:
        search_queries = []
        embedding_names = []
        
        requests = [r["request"] for r in batch]
        for r in requests:
            embedding_name = f"embedding_{r.embedding.plugin_id}"
            query = {
                "query": r.embedding.embedding,
                "using": embedding_name,
                "limit": r.k,
                "with_vector": True,
                "with_payload": True
            }
            search_queries.append(query)
            embedding_names.append(embedding_name)
            
        payload = {"searches": search_queries}
        self.last_embedding_names = embedding_names
        return payload

    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        results_batch = []
        for i, result in enumerate(http_resp["result"]):
            results = []
            for point in result["points"]:
                embedding = point["vector"][self.last_embedding_names[i]]
                payload = point["payload"]
                norm = payload.get('norm', None)
                if norm is not None:
                    embedding = (np.array(embedding) * norm).tolist()
                    
                result_data = {
                    "external_id": payload.get("external_id", 0),
                    "item":        payload.get("item", ""),
                    "embedding":   embedding,
                    "distance":    point["score"]
                }
                results.append(result_data)
            results_batch.append({"valid": bool(results), "result": results})
        return results_batch


# ---------------------------------------------------------------------------
# 5. Routing ----------------------------------------------------------------
# ---------------------------------------------------------------------------

def get_formatter(plugin: PluginInDB):
    fmt_cls = GenericFormatter
    if plugin.plugin_class == PluginClass.INTERNAL_TEI:
        fmt_cls = TEIFormatter
    elif plugin.plugin_class == PluginClass.INTERNAL_TRITON:
        if plugin.type == PluginType.EMBEDDING:
            fmt_cls = TritonEmbeddingFormatter
        elif plugin.type == PluginType.MAPPER:
            fmt_cls = TritonMapperFormatter
    elif plugin.plugin_class == PluginClass.INTERNAL_QDRANT:
        fmt_cls = QdrantFormatter

    return fmt_cls(plugin)
