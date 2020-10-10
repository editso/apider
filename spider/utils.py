import os
import json
import requests
import base64
import logging as _logging
import time
import hashlib
import random

logging = _logging.getLogger(__name__)


class DynamicAttributes(object):

    def __init__(self, attr_dict: dict):
        self.__dict__.update(attr_dict)

    def attrs(self, attrs: dict):
        self.__dict__.update(attrs)

    def __getattr__(self, item):
        return self.__dict__.get(item)

    def __setattr__(self, key, value):
        self.__dict__.__setattr__(key, value)

    def __repr__(self):
        return str(self.__dict__)


def dynamic_attr(attr_dict):
    return DynamicAttributes(attr_dict)


def save(path, filename, data):
    file = os.path.join(path, filename)
    with open(file, "w+", encoding='utf8') as stream:
        stream.write(data)
        stream.close()


def load_json(*file):
    file = os.path.join(*file)
    try:
        data = None
        with open(file, "r", encoding="utf8") as file:
            data = json.loads(file.read())
            file.close()
        return data
    except json.decoder.JSONDecodeError as e:
        return None


def catcher(capture=True, default_value=None):
    def wrapper(invoke):
        def try_catch(*args, **kwargs):
            try:
                return invoke(*args, **kwargs)
            except Exception as e:
                _capture = kwargs.get("catcher_capture", capture)
                _default = kwargs.get("catcher_default", default_value)
                if _capture and callable(invoke):
                    logging.debug("%s:%s args: %s, kwargs: %s" % (invoke.__name__, str(e), str(args), str(kwargs)))
                    return _default
                raise e

        return try_catch

    return wrapper


def sleep_range(max_rang=5):
    try:
        max_rang = abs(int(max_rang))
    except Exception:
        max_rang = 5
    sec = max([1, random.randrange(max_rang)])
    time.sleep(sec)


def remove_url_end(url):
    url = str(url)
    while len(url) > 1 and url[-1] == '/':
        url = url[:-1]
    return url


def image_base64(url, *args, **kwargs):
    data = requests.get(url, *args, **kwargs)
    if not data.ok:
        return None
    return base64.b64encode(data.content).decode(encoding="utf8")


def get_localtime(mat='%Y-%m-%d/%H:%M:%S'):
    return time.strftime(mat, time.localtime())


def md5_hex_digest(s: str, encode='utf8'):
    return hashlib.md5(s.encode(encode)).hexdigest()

