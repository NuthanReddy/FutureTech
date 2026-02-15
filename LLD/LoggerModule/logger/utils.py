from enum import Enum
from datetime import datetime


class LogLevel(Enum):
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4
    FATAL = 5


class Message:
    def __init__(self, content: str, level: LogLevel):
        self.content = content
        self.level = level
        self.timestamp = datetime.now()

    def __lt__(self, other):
        return self.level.value < other.level.value
