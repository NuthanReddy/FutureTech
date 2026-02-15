import os
import threading
from abc import ABC, abstractmethod
from .utils import LogLevel


class Sink(ABC):
    def __init__(self, level: LogLevel, max_message_size: int):
        self.level = level
        self.max_message_size = max_message_size

    @abstractmethod
    def write(self, message: str):
        pass


class ConsoleSink(Sink):
    def write(self, message: str):
        print(message)


class FileSink(Sink):
    def __init__(self, level: LogLevel, max_message_size: int, file_path: str):
        super().__init__(level, max_message_size)
        self.file_path = file_path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

    def write(self, message: str):
        with self.lock:
            with open(self.file_path, 'a') as f:
                f.write(message + '\n')
