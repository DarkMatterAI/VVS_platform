# vvs_database.execution.execution_strategy.formatters.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union

from vvs_database.schemas import PluginInDB, PluginClass

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
        tei_data = {"inputs": [i["request"].item_data.item for i in batch]}
        tei_data.update(self.plugin.config)
        print(tei_data)
        return tei_data 
    
    def parse_response(self, 
                       http_resp: Any,
                       batch_size: int 
                       ) -> List[Dict]:
        http_resp = [{"embedding": i, "valid": True} for i in http_resp]
        return http_resp 

# # ---------------------------------------------------------------------------
# # 3. Triton embedding / mapper ----------------------------------------------
# # ---------------------------------------------------------------------------
# class TritonFormatter(BaseFormatter):
#     def __init__(self, model_name: str):
#         self.model = model_name

#     # decide once whether this model is “embedding” or “mapper”
#     def _is_mapper(self):
#         return self.model.startswith("mapper")

#     def build_payload(self, batch):
#         if self._is_mapper():
#             emb = [r.embedding.embedding for r in batch]
#             return {
#                 "inputs": [
#                     {
#                         "name": "embedding",
#                         "shape": [len(batch), len(emb[0])],
#                         "datatype": "FP32",
#                         "data": emb,
#                     }
#                 ]
#             }
#         else:  # embedding
#             return {
#                 "inputs": [
#                     {
#                         "name": "sequence",
#                         "shape": [len(batch), 1],
#                         "datatype": "BYTES",
#                         "data": [r.item_data.item for r in batch],
#                     }
#                 ]
#             }

#     def parse_response(self, http_resp, batch_size):
#         data = http_resp["outputs"][0]["data"]
#         shape = http_resp["outputs"][0]["shape"]
#         if self._is_mapper():           # shape = [bs, N, d]
#             bs, n_out, d = shape
#             out = []
#             for i in range(bs):
#                 embeddings = [
#                     data[i * n_out * d + j * d : i * n_out * d + (j + 1) * d]
#                     for j in range(n_out)
#                 ]
#                 out.append(ExecuteResponse(valid=True, embedding=embeddings))
#             return out
#         else:                           # embedding model
#             bs, d = shape               # type: ignore
#             return [
#                 ExecuteResponse(
#                     valid=True,
#                     embedding=data[i * d : (i + 1) * d],
#                 )
#                 for i in range(bs)
#             ]

# ---------------------------------------------------------------------------
# 4. Routing ----------------------------------------------------------------
# ---------------------------------------------------------------------------

def get_formatter(plugin: PluginInDB):
    fmt_cls = GenericFormatter
    if plugin.plugin_class == PluginClass.INTERNAL_TEI:
        fmt_cls = TEIFormatter

    return fmt_cls(plugin)

