
import torch 
import numpy as np 
from sentence_transformers import models, SentenceTransformer
from transformers import AutoModel
from pytriton.decorators import batch, group_by_keys

from logger import logger
from utils import parse_sequence_batch
from config import EMBED_CONFIG, DECOMPOSER_CONFIG, DEVICES

class InferenceWrapper():
    embed_config = EMBED_CONFIG
    decomposer_config = DECOMPOSER_CONFIG
    def __init__(self, device):
        self.device = device
        self.load_models()

    def load_models(self):
        self.load_embedding_model() 
        self.load_decomposer()

    def load_embedding_model(self):
        transformer = models.Transformer(self.embed_config.hf_name,
                                         self.embed_config.max_seq_length,
                                         model_args={"add_pooling_layer": False})
        pooling = models.Pooling(transformer.get_word_embedding_dimension(), 
                                 pooling_mode="mean")
        self.embedding_model = SentenceTransformer(modules=[transformer, pooling],
                                                   device=self.device)
        self.embedding_model.eval()

    def load_decomposer(self):
        self.decomposer = AutoModel.from_pretrained(self.decomposer_config.hf_name,
                                                    trust_remote_code=True)
        self.decomposer.eval()
        self.decomposer.to(self.device)

    @group_by_keys
    @batch 
    def embed(self, sequence, **size_kwargs):
        output_size = EMBED_CONFIG.parse_compression_size(size_kwargs)
        inputs = parse_sequence_batch(sequence)
        logger.info(f"Embedding {len(inputs)} items to size {output_size}, device {self.device}")
        
        # with torch.inference_mode(), torch.autocast(device_type=self.device):
        with torch.inference_mode():
            embeddings = self.embedding_model.encode(inputs, 
                                                     batch_size=self.embed_config.batch_size,
                                                     show_progress_bar=False,
                                                     convert_to_tensor=True)
            if embeddings.shape[-1] != output_size:
                embedding_dict = self.decomposer.compress(embeddings, [output_size])
                embeddings = embedding_dict[output_size]
        print(embeddings.shape)
        return {"embedding": embeddings.cpu().numpy().astype(np.float32)}

    @group_by_keys
    @batch 
    def decompose(self, embedding, **size_kwargs):
        input_size, output_size = DECOMPOSER_CONFIG.parse_decomposer_sizes(size_kwargs)
        logger.info(f"Decomposing {embedding.shape[0]} items {input_size}->{output_size}, device {self.device}")

        # with torch.inference_mode(), torch.autocast(device_type=self.device):
        with torch.inference_mode():
            e_dict = {input_size: torch.from_numpy(embedding).float().to(self.device)}
            d_dict = self.decomposer.decompose(e_dict, [output_size]) # {output_size: [1, B, 2, output_size]}
            decomposed = d_dict[output_size][0]
        print(decomposed.shape)
        return {"embedding": decomposed.detach().cpu().numpy().astype(np.float32)}

MODELS = [InferenceWrapper(i) for i in DEVICES]

def _embed_factory():
    return [model.embed for model in MODELS]

def _decompose_factory():
    return [model.decompose for model in MODELS]
