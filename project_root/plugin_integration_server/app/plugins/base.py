from abc import ABC, abstractmethod

class BasePlugin(ABC):
    @abstractmethod
    async def process(self, request):
        pass