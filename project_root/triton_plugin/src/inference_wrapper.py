import torch 

import numpy as np 

from transformers import RobertaModel, AutoTokenizer, AutoConfig, AutoModel

from pytriton.decorators import batch

from logger import logger

EMBBEDDING_MODEL_NAME = 'entropy/roberta_zinc_480m'
COMPRESSION_MODEL_NAME = 'entropy/roberta_zinc_compression_head'
MAPPER_64_MODEL_NAME = 'entropy/enamine_embedding_mapper'

TOKENIZER = AutoTokenizer.from_pretrained(EMBBEDDING_MODEL_NAME)

COMPRESSION_CONFIG = AutoConfig.from_pretrained(COMPRESSION_MODEL_NAME, 
                                                trust_remote_code=True)
EMBEDDING_SIZE = COMPRESSION_CONFIG.input_size
COMPRESSION_HEAD_SIZES = COMPRESSION_CONFIG.compression_sizes

MAPPER_64_CONFIG = AutoConfig.from_pretrained(MAPPER_64_MODEL_NAME, 
                                              trust_remote_code=True)
MAPPER_OUTPUT_SHAPE = (MAPPER_64_CONFIG.n_out, MAPPER_64_CONFIG.d_out)

if torch.cuda.is_available():
    DEVICES = [f'cuda:{i}' for i in range(torch.cuda.device_count())]
else:
    DEVICES = ['cpu']

def compute_embedding(features, mask):
    if mask is None:
        mask = torch.ones(features.shape[0], features.shape[1], device=features.device)

    embeddings = ((features * mask.unsqueeze(-1)).sum(1) / mask.sum(-1).unsqueeze(-1))
    return embeddings

def tokenize_inputs(sequence_batch, device):
    sequence_batch = np.char.decode(sequence_batch.astype("bytes"), "utf-8")

    inputs = []
    for sequence_item in sequence_batch:
        inputs.append(sequence_item.item())

    batch = TOKENIZER(inputs, return_tensors='pt', padding=True, pad_to_multiple_of=8)
    batch = {k:v.to(device) for k,v in batch.items()}
    return batch 

def embed(sequence, embedding_size, device, encoder, compression_heads):
    batch = tokenize_inputs(sequence, device)
    logger.info(f"Embedding size {embedding_size}, device {device}, {batch['input_ids'].shape[0]} inputs")

    with torch.inference_mode(), torch.autocast(device_type=device):
        results = encoder(**batch, output_hidden_states=True)
        embeddings = compute_embedding(results.hidden_states[-1], batch['attention_mask'])
        head = compression_heads.get(embedding_size, None)
        if head:
            embeddings = head(embeddings)

    return {'embedding' : embeddings.detach().cpu().numpy().astype(np.float32)}

def embedding_mapper(embedding, embedding_size, device, mapper_model):
    logger.info(f"Enamine Mapper input size {embedding_size}, device {device}, {embedding.shape[0]} inputs")

    with torch.inference_mode(), torch.autocast(device_type=device):
        embedding = torch.from_numpy(embedding).float().to(device)
        outputs = mapper_model(embedding)

    return {'embeddings' : outputs.detach().cpu().numpy().astype(np.float32)} 

class InferenceWrapper():
    def __init__(self, device):
        self.device = device
        self.load_models()

    def load_models(self):
        self.load_embedding_model() 
        self.load_compression_heads()
        self.load_enamine_mapper_64()

    def load_embedding_model(self):
        self.base_embedding = RobertaModel.from_pretrained(EMBBEDDING_MODEL_NAME, 
                                                           add_pooling_layer=False)
        self.base_embedding.eval()
        self.base_embedding.to(self.device)

    def load_compression_heads(self):
        self.compression_heads = {}
        compression_model = AutoModel.from_pretrained(COMPRESSION_MODEL_NAME,
                                                      trust_remote_code=True)
        for idx, size in enumerate(compression_model.config.compression_sizes):
            head = compression_model.heads[idx]
            head.eval()
            head.to(self.device)
            self.compression_heads[size] = head

    def load_enamine_mapper_64(self):
        mapper_model = AutoModel.from_pretrained(MAPPER_64_MODEL_NAME,
                                                 trust_remote_code=True)
        self.enamine_mapper_64_model = mapper_model.mapper
        self.enamine_mapper_64_model.eval()
        self.enamine_mapper_64_model.to(self.device)

    @batch
    def embed_768(self, sequence):
        return embed(sequence, 768, self.device, self.base_embedding, self.compression_heads)
    
    @batch
    def embed_512(self, sequence):
        return embed(sequence, 512, self.device, self.base_embedding, self.compression_heads)
    
    @batch
    def embed_256(self, sequence):
        return embed(sequence, 256, self.device, self.base_embedding, self.compression_heads)
    
    @batch
    def embed_128(self, sequence):
        return embed(sequence, 128, self.device, self.base_embedding, self.compression_heads)
    
    @batch
    def embed_64(self, sequence):
        return embed(sequence, 64, self.device, self.base_embedding, self.compression_heads)
    
    @batch
    def embed_32(self, sequence):
        return embed(sequence, 32, self.device, self.base_embedding, self.compression_heads)
    
    @batch
    def enamine_mapper_64(self, embedding):
        return embedding_mapper(embedding, 64, self.device, self.enamine_mapper_64_model)


MODELS = [InferenceWrapper(i) for i in DEVICES]

def _embed_factory(embedding_size):
    return [getattr(model, f"embed_{embedding_size}") for model in MODELS]

def _enamine_mapper_factory():
    return [model.enamine_mapper_64 for model in MODELS]

