import os
from .base import BasePlugin
from ..utils import post_request
from ..config import PLUGIN_CONFIG

class TeiPlugin(BasePlugin):
    async def process(self, request):
        tei_data = {
            'inputs': request.item,
            'normalize': False if os.environ.get('TEI_NORMALIZE', 'false')=='false' else True,
            'truncate': False if os.environ.get('TEI_TRUNCATE', 'false')=='false' else True,
            'truncation_direction': os.environ.get('TEI_TRUNCATION_DIRECTION', 'right')
        }
        response = await post_request(tei_data, PLUGIN_CONFIG['tei'])
        return {'embedding': response[0]}