from logging import *
import json
from elasticsearch import Elasticsearch
from .utils import dynamic_attr
import urllib3

urllib3.disable_warnings()


class Storage:
    """
    存储基类
    """

    def save(self, name, data):
        """
        子类实现保存方法
        """
        pass

    def save_stream(self, name, binary: bytes):
        """
        数据流文件
        """
        pass

    def query(self, query, *args, **kwargs):
        pass

    def exists(self, index, unique, *args, **kwargs):
        pass


class ElasticStorage(Storage):

    def __init__(self, hosts=None,
                 ports=None, user=None,
                 password=None,
                 scheme="http",
                 verify_certs=False,
                 type_name="data"):
        self._es = Elasticsearch(hosts,
                                 http_auth=(user, password),
                                 scheme=scheme,
                                 verify_certs=verify_certs,
                                 ssl_show_warn = None,
                                 ports=ports)

    def index_exists(self, index):
        return self._es.indices.exists(index=index)

    def index_create(self, index):
        if not self.index_exists(index):
            self._es.indices.create(index)

    def save(self, index, data, e_id=None):
        self.index_create(index)
        self._es.index(index=index, id=e_id, body=data)
        self.force_update(index)

    def put_mapping(self, index, properties):
        self._es.indices.put_mapping(index=index, body=properties)

    def save_stream(self, name, binary: bytes):
        pass

    def exists(self, index, query, *args, **kwargs):
        data = dynamic_attr(self.query(index, query=query))
        if data._shards['total'] >= 1:
            return True
        return False

    def force_update(self, index):
        self._es.indices.flush(index=index, force=True)

    def update(self, index, e_id, body):
        self.index_create(index)
        self._es.update(index, e_id, body={
            'doc': body
        })
        self.force_update(index)

    def update_all(self, index, hits: list, update_terms):
        for item in hits:
            item = dynamic_attr(item)
            self.update(index, item._id, update_terms)

    def query(self, index, *args, **kwargs):
        query = kwargs.get('query', {})
        try:
            del kwargs['query']
        except Exception:
            pass
        return self.search(index, query=query)

    def search(self, index, query, *args, **kwargs):
        self.index_create(index)
        return self._es.search(index=index, body={
            'query': query
        }, *args, **kwargs)

    def terms_query(self, index, query, *args, **kwargs):
        return self.search(index=index, query={
            'terms': query
        }, *args, **kwargs)

    def match_query(self, index, query, *args, **kwargs):
        return self.search(index=index, query={
            'match': query
        }, *args, **kwargs)

    def term_query(self, index, query, *args, **kwargs):
        return self.search(index, query={
            'term': query
        }, *args, **kwargs)

    def get(self, index, e_id, *args, **kwargs):
        try:
            return self._es.get(index, id=e_id, *args, **kwargs)
        except Exception:
            return None


class Cache(object):
    """
    缓存
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def push(self, value):
        pass

    def pop(self):
        pass

    def empty(self):
        pass

    def size(self):
        pass


class Spider(object):
    _storage = None
    _cache = None

    def __init__(self, name, storage, cache):
        self._name = name
        self._storage = storage
        self._cache: Cache = cache

    @property
    def cache(self):
        return self._cache

    @property
    def name(self):
        return self._name

    @property
    def storage(self):
        return self._storage

    @storage.setter
    def storage(self, storage):
        self._storage = storage

    def save(self, data):
        if isinstance(data, str):
            data = json.loads(data)
        elif isinstance(data, set):
            data = list(data)
        self.storage.save(self.name, data)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            error("Spider Error")

    def quit(self):
        exit(0)

    def start(self):
        pass
