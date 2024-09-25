import logging
import os 
import numpy as np
import torch  # pytype: disable=import-error

from pytriton.decorators import batch
from pytriton.model_config import ModelConfig, DynamicBatcher, Tensor
from pytriton.triton import Triton, TritonConfig

from mapper_model import MLPMapper

mapper_config = {
    'd_in' : 768,
    'n_in' : 1,
    'd_hidden' : 2048,
    'n_layers' : 6,
    'd_out' : 768,
    'n_out' : 2,
    'bn' : True,
    'dropout' : 0.0
}

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
# MODEL = torch.nn.Linear(20, 30).to(DEVICE).eval()
MODEL = MLPMapper(**mapper_config).to(DEVICE).eval()

HOST = os.environ.get('TRITON_HOST', "0.0.0.0")
HTTP_PORT = int(os.environ.get('TRITON_HTTP_PORT', 8000))
METRICS_PORT = int(os.environ.get('TRITON_METRICS_PORT', 8002))
BATCH_SIZE = int(os.environ.get('TRITON_MAX_BATCH_SIZE', 512))
MAX_DELAY = int(os.environ.get('TRITON_MAX_QUEUE_DELAY', 5000))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s: %(message)s")
logger = logging.getLogger("examples.linear_random_pytorch.server")

@batch
def _infer_fn(**inputs):
    embeddings = inputs['embedding']
    input_batch_tensor = torch.from_numpy(embeddings).to(DEVICE)
    output_batch_tensor = MODEL(input_batch_tensor)
    output_batch = output_batch_tensor.cpu().detach().numpy()
    return [output_batch]


triton_config = TritonConfig(http_address=HOST,
                             http_port=HTTP_PORT,
                             metrics_port=METRICS_PORT,
                             )

with Triton(config=triton_config) as triton:
    logger.info("Loading Mapper model")

    triton.bind(
        model_name="Mapper",
        infer_func=_infer_fn,
        inputs=[
            Tensor(name="embedding", dtype=np.float32, shape=(-1,)),
        ],
        outputs=[
            Tensor(name="result", dtype=np.float32, shape=(mapper_config['n_out'], -1,))
        ],
        config=ModelConfig(max_batch_size=BATCH_SIZE,
                           batcher=DynamicBatcher(max_queue_delay_microseconds=MAX_DELAY)),
        strict=True,
    )
    logger.info("Serving models")
    triton.serve()
