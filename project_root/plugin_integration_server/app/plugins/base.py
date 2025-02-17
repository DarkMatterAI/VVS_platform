from abc import ABC, abstractmethod

class BasePlugin(ABC):
    @abstractmethod
    async def _process(self, request, **kwargs):
        pass

    async def process(self, request, **kwargs):
        delist = False 
        if type(request) != list:
            request = [request]
            delist = True 

        response = await self._process(request, **kwargs)
        if delist:
            response = response[0]

        return response 