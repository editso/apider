from logging import *
from urllib import parse
from urllib.error import HTTPError
import json
import requests
from elasticsearch import Elasticsearch
from base64 import b64decode
from .utils import dynamic_attr


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
                                 ports=ports)

    def save(self, index, data):
        self._es.index(index=index, body=data)

    def put_mapping(self, index, properties):
        self._es.indices.put_mapping(index=index, body=properties)

    def save_stream(self, name, binary: bytes):
        pass

    def exists(self, index, query, *args, **kwargs):
        data = dynamic_attr(self.query(index, query=query))
        if data._shards['total'] >= 1:
            return True
        return False

    def query(self, index, *args, **kwargs):
        query = kwargs.get('query', {})
        del kwargs['query']
        return self._es.search(index=index, body={
            'query': {
                'term': query
            }
        }, *args, **kwargs)


class Cache(object):
    """
    缓存
    """

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
