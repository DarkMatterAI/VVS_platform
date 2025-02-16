import os

PLUGIN_CONFIG = {
    'tei': {
        'url': f"http://tei_plugin:{os.environ.get('TEI_PORT', '')}/embed",
        'timeout': 120,
        'retries': 3
    },
    'triton' : {
        'base_url': f"http://triton_plugin:{os.environ.get('TRITON_HTTP_PORT', '')}/v2/models",
        'timeout': 120,
        'retries': 3,
        'model_names' : {
            'mapper' : ['ENAMINE_MAPPER_64'],
            'embedding' : [f'EMBED_{i}' for i in [768, 512, 256, 128, 64, 32]]
        }
    }
}