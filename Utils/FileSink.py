from logger.Utils.Sink import Sink
from logger.logger import LogLevel
import threading, os


class FileSink(Sink):
    def __init__(self, level: LogLevel, max_message_size: int, file_path: str):
        super().__init__(level, max_message_size)
        self.file_path = file_path
        self.lock = threading.Lock()

        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

    def write(self, message: str):
        with self.lock:
            with open(self.file_path, 'a') as f:
                f.write(message + '\n')