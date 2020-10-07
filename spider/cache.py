from .spider import Cache
from queue import Queue
import redis


class RedisCache(Cache):
    _redis = None

    def __init__(self, cache_name, user, password, port=6379, db=0, host='0.0.0.0', *args, **kwargs):
        self.cache_name = cache_name
        self._redis = redis.StrictRedis(username=user,
                                        password=password,
                                        host=host,
                                        port=port, db=db, *args, **kwargs)

    def push(self, value):
        self._redis.sadd(self.cache_name, value)

    def pop(self):
        return self._redis.spop(self.cache_name).decode("utf8")

    def size(self):
        return self._redis.scard(self.cache_name)

    def empty(self):
        return self.size() <= 0


class QueueCache(Cache):

    def __init__(self):
        self._queue = Queue()

    def pop(self):
        return self._queue.get()

    def push(self, value):
        self._queue.put(value)

    def empty(self):
        return self._queue.empty()

    def size(self):
        return self._queue.qsize()