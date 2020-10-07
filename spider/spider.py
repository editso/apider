from logging import *
from urllib.request import Request, urlopen
from urllib import parse
from urllib.error import HTTPError
import json
import ssl
import logging
import requests


class Storage:
    """
    存储基类
    """

    def save(self, name, data):
        """
        子类实现保存方法
        """
        pass


class ElasticStorage(Storage):

    def __init__(self, host=None, port=None, user=None, password=None, schema="http", type_name="data"):
        self.host = host
        self.port = port
        self.schema = schema
        self.type_name = type_name
        self.user = user
        self.password = password
        self._req = requests.Request(self._url())

    def _url(self, path=None):
        url = None
        if self.user:
            url = "{}://{}:{}@{}:{}".format(self.schema, self.user, self.password, self.host, self.port)
        else:
            url = "{}://{}:{}".format(self.schema, self.host, self.port)
        return parse.urljoin(url, path)

    def url(self):
        return self._url()

    def _execute(self, url, method="GET", **kwargs):
        """
        发送请求
        """
        try:
            self._req.url = url
            self._req.method = method
            return requests.request(method=method, url=url, **kwargs, verify=False, headers=self._req.headers)
        finally:
            self.reset()

    def reset(self):
        self._req.full_url = self._url(None)

    def index_exists(self, name):
        try:
            data = self._execute(self._url(name))
            code = data.json().get('status')
            if code == 404:
                return False
            return True
        except HTTPError as e:
            data = json.loads(e.read().decode("utf8"))
            if data["status"] == 404:
                return False

    def index_create(self, name):
        try:
            self._execute(self._url(name), method="PUT")

        except HTTPError as e:
            error("Index Create Error", e)

    def save(self, name, data):
        if not self.index_exists(name):
            self.index_create(name)
        try:
            self._req.headers["content-type"] = "application/json"
            data = self._execute(self._url("{}/{}".format(name, self.type_name)),
                                 method="POST",
                                 data=bytes(json.dumps(data, ensure_ascii=False), "utf8")).json()
            print(data)
            # self._req.remove_header("content-type")
        except HTTPError as e:
            error("Save Error", e.read())


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
