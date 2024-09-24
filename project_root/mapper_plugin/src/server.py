import logging

import numpy as np
import torch  # pytype: disable=import-error

from pytriton.decorators import batch
from pytriton.model_config import ModelConfig, Tensor
from pytriton.triton import Triton

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MODEL = torch.nn.Linear(20, 30).to(DEVICE).eval()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s: %(message)s")
logger = logging.getLogger("examples.linear_random_pytorch.server")

@batch
def _infer_fn(**inputs):
    logger.info("embedding endpoint")
    embeddings = inputs['embedding']
    input_batch_tensor = torch.from_numpy(embeddings).to(DEVICE)
    output_batch_tensor = MODEL(input_batch_tensor)
    output_batch = output_batch_tensor.cpu().detach().numpy()
    return [output_batch]


with Triton() as triton:
    logger.info("Loading Linear model.")
    logger.info("Embedding changes registered")
    triton.bind(
        model_name="Linear",
        infer_func=_infer_fn,
        inputs=[
            Tensor(name="embedding", dtype=np.float32, shape=(-1,)),
        ],
        outputs=[
            Tensor(name="result", dtype=np.float32, shape=(-1,))
        ],
        config=ModelConfig(max_batch_size=128),
        strict=True,
    )
    logger.info("Serving models")
    triton.serve()