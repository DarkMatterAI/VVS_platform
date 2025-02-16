from .rdkit_plugin import init_rdkit_records
from .tei_plugin import init_tei_records
from .qdrant_plugin import init_qdrant_records
from .triton_plugin import init_triton_records

PLUGIN_CREATE_DICT = {
    'tei_plugin' : {
        'plugin_class' : 'internal_tei',
        'create_func' : init_tei_records
    },
    'qdrant_plugin' : {
        'plugin_class' : 'internal_qdrant',
        'create_func' : init_qdrant_records
    },
    'rdkit_plugin' : {
        'plugin_class' : 'internal_rdkit',
        'create_func' : init_rdkit_records
    },
    'triton_plugin' : {
        'plugin_class' : 'internal_triton',
        'create_func' : init_triton_records
    }
}
