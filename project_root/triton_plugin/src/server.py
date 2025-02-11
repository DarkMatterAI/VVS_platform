import os 
import numpy as np 

from pytriton.triton import Triton, TritonConfig
from pytriton.model_config import ModelConfig, DynamicBatcher, Tensor

from logger import logger 
from inference_wrapper import INPUT_SIZE, HEAD_SIZES, _embed_factory

HOST = os.environ.get('TRITON_HOST', "0.0.0.0")
HTTP_PORT = int(os.environ.get('TRITON_HTTP_PORT', 8000))
METRICS_PORT = int(os.environ.get('TRITON_METRICS_PORT', 8002))
EMBED_BATCH_SIZE = int(os.environ.get('TRITON_EMBED_BATCH_SIZE', 512))
MAX_DELAY = int(os.environ.get('TRITON_MAX_QUEUE_DELAY', 5000))

triton_config = TritonConfig(http_address=HOST,
                             http_port=HTTP_PORT,
                             metrics_port=METRICS_PORT,
                             )

with Triton(config=triton_config) as triton:
    logger.info("Loading models")
    for embedding_size in [INPUT_SIZE] + HEAD_SIZES:
        triton.bind(
            model_name=f"EMBED_{embedding_size}",
            infer_func=_embed_factory(embedding_size),
            inputs=[
                Tensor(name="sequence", dtype=np.bytes_, shape=(1,)),
            ],
            outputs=[
                Tensor(name="embedding", dtype=np.float32, shape=(-1,))
            ],
            config=ModelConfig(max_batch_size=EMBED_BATCH_SIZE,
                            batcher=DynamicBatcher(max_queue_delay_microseconds=MAX_DELAY)),
            strict=True,
        )
    logger.info("Serving inference")
    triton.serve()

