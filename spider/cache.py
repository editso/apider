from .spider import Cache
from queue import Queue
import redis
from spider.spider import ElasticStorage
from .utils import dynamic_attr, get_localtime, md5_hex_digest
import json
import threading


class ElasticCache(Cache):
    stat = {
        'wait': 'wait',
        'queue': 'queue',
        'failure': 'failure',
        'success': 'success'
    }

    def __init__(self, cache_name, max_size=10, elastic=None, *args, **kwargs):
        self._cache = ElasticStorage(**elastic)
        self._cache_name = cache_name
        self._cur_data = QueueCache()
        self._dynamic = dynamic_attr(self.stat)
        self._max_size = max_size
        self._lock = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.size() > 0:
            self.reset_data(self._cache_name)

    def reset_data(self, index=None):
        while self._cur_data.size() > 0:
            try:
                data = self._cur_data.pop()
                self._cache.update(index or self._cache_name, data['_id'], {
                    'cache_stat': self._dynamic.wait
                })
            except Exception:
                pass

    def _update(self, value, stat, *args, **kwargs):
        self._lock.acquire()
        index = kwargs.get('cache_name', self._cache_name)
        e_id = md5_hex_digest(json.dumps(value, ensure_ascii=False))
        data = self._cache.get(index, e_id=e_id, _source=True)
        if not data:
            self._cache.save(index, {
                'date': get_localtime(),
                'cache_stat': stat,
                'data': value
            }, e_id=e_id)
        self._lock.release()

    def push(self, value, **kwargs):
        self._update(value, self._dynamic.wait, **kwargs)

    def _load_elastic_data(self, index, auto_update=True, **kwargs):
        data = self._cache.terms_query(index, query={
            'cache_stat': [
                self._dynamic.wait,
                self._dynamic.failure
            ],
        }, size=self._max_size)
        data = dynamic_attr(data)
        for item in data.hits['hits']:
            self._cur_data.push(item['_source']['data'])
            if auto_update:
                self._cache.update(index, item['_id'], {
                    'cache_stat': self._dynamic.queue,
                    'date': get_localtime()
                })

    def pop(self, auto_update=True, **kwargs):
        self._lock.acquire()
        try:
            return self._cur_data.pop() if self.size(auto_update=auto_update) > 0 else None
        finally:
            self._lock.release()

    def success(self, value, **kwargs):
        self._update(value, self._dynamic.success)

    def error(self, value, **kwargs):
        self._update(value, self._dynamic.failure)

    def size(self, **kwargs):
        if self._cur_data.size() <= 0:
            index = kwargs.get('cache_name', self._cache_name)
            self._load_elastic_data(index, **kwargs)
        return self._cur_data.size()


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


def elastic_cache(*args, **kwargs) -> ElasticCache:
    return ElasticCache(*args, **kwargs)
