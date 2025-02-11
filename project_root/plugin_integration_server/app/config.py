import os

PLUGIN_CONFIG = {
    'tei': {
        'url': f"http://tei_plugin:{os.environ.get('TEI_PORT', '')}/embed",
        'timeout': 120,
        'retries': 3
    },
    'mapper': {
        'url': f"http://mapper_plugin:{os.environ.get('TRITON_HTTP_PORT', '')}/v2/models/Mapper/infer",
        'timeout': 120,
        'retries': 3
    }
}