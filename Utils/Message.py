from LogLevel import LogLevel
from datetime import datetime


class Message:
    def __init__(self, content: str, level: LogLevel):
        self.content = content
        self.level = level
        self.timestamp = datetime.now()

    def __lt__(self, other):
        return self.level.value < other.level.value
