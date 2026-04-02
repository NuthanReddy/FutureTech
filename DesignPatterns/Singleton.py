import sqlite3
import threading


class Singleton(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Singleton, cls).__new__(cls)
        return cls.instance


s1 = Singleton()
s2 = Singleton()

print(s1 is s2)


class DatabaseSingleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabaseSingleton, cls).__new__(cls)
                    cls._instance.conn = sqlite3.connect("database.db")
        return cls._instance

# https://softwarepatterns.com/python/singleton-software-pattern-python-example