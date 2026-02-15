from logger.Utils import LogLevel
from abc import ABC, abstractmethod


class Sink(ABC):
    def __init__(self, level: LogLevel, max_message_size: int):
        self.level = level
        self.max_message_size = max_message_size

    @abstractmethod
    def write(self, message: str):
        pass