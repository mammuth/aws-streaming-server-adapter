from abc import ABC, abstractmethod

# Import AWS Elemental Adapter as `StreamingServerAdapter` since it's the only implementation of the adapter right now
from .aws_elemental import AbstractStreamingServerAdapter as StreamingServerAdapter


class AbstractStreamingServerAdapter(ABC):
    @abstractmethod
    def create_channel(self, slug: str, streamkey: str, start: bool = True, debug: bool = False) -> dict:
        pass

    @abstractmethod
    def stop_channel(self, channel_id: str):
        pass

    @abstractmethod
    def delete_idle_channels(self):
        pass

    @abstractmethod
    def stop_unused_channels(self):
        pass

    @abstractmethod
    def stop_long_running_channels(self):
        pass
