import threading, heapq
from typing import List, Dict, Any
from .utils import LogLevel, Message
from .sinks import Sink


class PriorityBuffer:
    def __init__(self, size: int):
        self.size = size
        self.buffer = []
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)

    def put(self, item):
        with self.lock:
            if len(self.buffer) < self.size:
                heapq.heappush(self.buffer, (-item.level.value, item))
            else:
                # When the buffer is full and a new message arrives,
                # it's only added if its priority is higher than the lowest priority message in the buffer.
                # This ensures that high-priority messages (ERROR, FATAL) are always preserved,
                # even in high-load scenarios.
                lowest_priority = self.buffer[0]
                if item.level.value > -lowest_priority[0]:
                    heapq.heapreplace(self.buffer, (-item.level.value, item))
            self.not_empty.notify()

    def get(self):
        with self.not_empty:
            while len(self.buffer) == 0:
                self.not_empty.wait()
            return heapq.heappop(self.buffer)[1]

    def full(self):
        with self.lock:
            return len(self.buffer) == self.size

    def empty(self):
        with self.lock:
            return len(self.buffer) == 0


class Logger:
    def __init__(self, name: str, sinks: List[Sink], buffer_size: int, is_async: bool, ts_format: str):
        self.name = name
        self.sinks = sinks
        self.buffer = PriorityBuffer(buffer_size)
        self.is_async = is_async
        self.ts_format = ts_format
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        if is_async:
            self.worker_thread = threading.Thread(target=self._process_buffer)
            self.worker_thread.daemon = True
            self.worker_thread.start()

    def log(self, message: Message):
        if self.is_async:
            self.buffer.put(message)
        else:
            self._process_message(message)

    def debug(self, content: str):
        self.log(Message(content, LogLevel.DEBUG))

    def info(self, content: str):
        self.log(Message(content, LogLevel.INFO))

    def warn(self, content: str):
        self.log(Message(content, LogLevel.WARN))

    def error(self, content: str):
        self.log(Message(content, LogLevel.ERROR))

    def fatal(self, content: str):
        self.log(Message(content, LogLevel.FATAL))

    def _process_buffer(self):
        while not self.stop_event.is_set():
            message = self.buffer.get()
            self._process_message(message)

    def _process_message(self, message: Message):
        formatted_message = self._format_message(message)
        for sink in self.sinks:
            if message.level.value >= sink.level.value:
                if len(formatted_message) <= sink.max_message_size:
                    sink.write(formatted_message)

    def _format_message(self, message: Message) -> str:
        timestamp = message.timestamp.strftime(self.ts_format)
        return f"{timestamp} [{message.level.name}] {message.content}"

    def stop(self):
        self.stop_event.set()
        if self.is_async:
            self.worker_thread.join(timeout=5)


class LoggerFactory:
    @staticmethod
    def create_logger(config: Dict[str, Any]) -> Logger:
        from .sinks import ConsoleSink, FileSink
        sinks = []
        for sink_config in config['sinks']:
            if sink_config['type'] == 'CONSOLE':
                sinks.append(ConsoleSink(LogLevel[sink_config['log_level']], sink_config['max_message_size']))
            elif sink_config['type'] == 'FILE':
                sinks.append(FileSink(LogLevel[sink_config['log_level']], sink_config['max_message_size'],
                                      sink_config['file_path']))

        return Logger(
            name=config['name'],
            sinks=sinks,
            buffer_size=config['buffer_size'],
            is_async=config['logger_type'] == 'ASYNC',
            ts_format=config['ts_format']
        )