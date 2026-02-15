import heapq
import threading


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