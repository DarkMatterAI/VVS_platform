from pytriton.triton import Triton, TritonConfig
from logger import logger 
from config import EMBED_CONFIG, DECOMPOSER_CONFIG, MODEL_SIZE_CONFIG, HOST, HTTP_PORT, METRICS_PORT
from inference_wrapper import _embed_factory, _decompose_factory

triton_config = TritonConfig(http_address=HOST,
                             http_port=HTTP_PORT,
                             metrics_port=METRICS_PORT,
                             )

with Triton(config=triton_config) as triton:
    logger.info("Loading models")
    triton.bind(
        model_name=EMBED_CONFIG.model_name,
        infer_func=_embed_factory(),
        inputs=EMBED_CONFIG.inputs,
        outputs=EMBED_CONFIG.outputs,
        config=EMBED_CONFIG.triton_config
    )
    triton.bind(
        model_name=DECOMPOSER_CONFIG.model_name,
        infer_func=_decompose_factory(),
        inputs=DECOMPOSER_CONFIG.inputs,
        outputs=DECOMPOSER_CONFIG.outputs,
        config=DECOMPOSER_CONFIG.triton_config
    )
    triton.bind(
        model_name=MODEL_SIZE_CONFIG.model_name,
        infer_func=MODEL_SIZE_CONFIG.__call__,
        inputs=MODEL_SIZE_CONFIG.inputs,
        outputs=MODEL_SIZE_CONFIG.outputs,
        config=MODEL_SIZE_CONFIG.triton_config
    )
    logger.info("Serving inference")
    triton.serve()

