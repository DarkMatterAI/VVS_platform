import os 
import torch 
from transformers import AutoConfig
from pytriton.model_config import ModelConfig, DynamicBatcher, Tensor
import numpy as np 
from utils import parse_size

EMBEDDING_NAME = "entropy/roberta_zinc_480m"
DECOMPOSER_NAME = "entropy/roberta_zinc_enamine_decomposer"
EMBED_BATCH_SIZE = int(os.environ.get('TRITON_EMBED_BATCH_SIZE', 512))
DECOMPOSER_BATCH_SIZE = int(os.environ.get('TRITON_DECOMPOSE_BATCH_SIZE', 512))

HOST = os.environ.get('TRITON_HOST', "0.0.0.0")
HTTP_PORT = int(os.environ.get('TRITON_HTTP_PORT', 8000))
METRICS_PORT = int(os.environ.get('TRITON_METRICS_PORT', 8002))
MAX_DELAY = int(os.environ.get('TRITON_MAX_QUEUE_DELAY', 5000))

if torch.cuda.is_available():
    DEVICES = [f'cuda:{i}' for i in range(torch.cuda.device_count())]
else:
    DEVICES = ['cpu']

class EmbedConfig():
    def __init__(self):
        self.hf_name = EMBEDDING_NAME
        self.max_seq_length = 256
        self.batch_size = EMBED_BATCH_SIZE
        self.config = AutoConfig.from_pretrained(DECOMPOSER_NAME, trust_remote_code=True)
        self.sizes = self.config.comp_sizes
        self.model_name = "EMBED"
        self.arg_prefix = "compress"
        self.triton_config = ModelConfig(max_batch_size=EMBED_BATCH_SIZE,
                                         batcher=DynamicBatcher(max_queue_delay_microseconds=MAX_DELAY))
        self.inputs = ([Tensor(name="sequence", shape=(1,), dtype=np.bytes_)] + 
                       [Tensor(name=f"{self.arg_prefix}_{size}", shape=(1,), dtype=np.bool_, optional=True)
                        for size in self.sizes])
        self.outputs = [Tensor(name="embedding", shape=(-1,), dtype=np.float32)]

    def parse_compression_size(self, size_kwargs):
        return parse_size(size_kwargs, self.sizes, self.arg_prefix)
    
class DecomposerConfig():
    def __init__(self):
        self.hf_name = DECOMPOSER_NAME
        self.batch_size = DECOMPOSER_BATCH_SIZE
        self.config = AutoConfig.from_pretrained(DECOMPOSER_NAME, trust_remote_code=True)
        self.input_sizes = self.config.comp_sizes
        self.output_sizes = self.config.output_sizes
        self.model_name = "DECOMPOSE"
        self.input_prefix = "input_size"
        self.output_prefix = "output_size"
        self.triton_config = ModelConfig(max_batch_size=DECOMPOSER_BATCH_SIZE,
                                         batcher=DynamicBatcher(max_queue_delay_microseconds=MAX_DELAY))
        self.inputs = (
            [Tensor(name="embedding", shape=(-1,), dtype=np.float32)] + 
            [Tensor(name=f"{self.input_prefix}_{size}", shape=(1,), dtype=np.bool_, optional=True)
             for size in self.input_sizes] + 
             [Tensor(name=f"{self.output_prefix}_{size}", shape=(1,), dtype=np.bool_, optional=True)
             for size in self.output_sizes]
        )
        self.outputs = [Tensor(name="embedding", shape=(2, -1), dtype=np.float32)]

    def parse_decomposer_sizes(self, size_kwargs):
        input_size = parse_size(size_kwargs, self.input_sizes, self.input_prefix)
        output_size = parse_size(size_kwargs, self.output_sizes, self.output_prefix)
        return input_size, output_size
    
class ModelSizeConfig():
    def __init__(self, input_sizes, output_sizes):
        self.model_name = "get_model_sizes"
        self.inputs = [Tensor(dtype=np.bool_, shape=(-1,))]
        self.outputs = [Tensor(name="mapper_input_sizes", dtype=np.int32, shape=(-1,)),
                        Tensor(name="mapper_output_sizes", dtype=np.int32, shape=(-1,))]
        self.triton_config = ModelConfig()
        self.output = {
            "mapper_input_sizes": np.array(input_sizes),
            "mapper_output_sizes": np.array(output_sizes)
        }

    def __call__(self, inputs):
        return [self.output]
    
EMBED_CONFIG = EmbedConfig()
DECOMPOSER_CONFIG = DecomposerConfig()
MODEL_SIZE_CONFIG = ModelSizeConfig(DECOMPOSER_CONFIG.input_sizes, DECOMPOSER_CONFIG.output_sizes)


