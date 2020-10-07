import socket
import json


class Request(object):

    def __init__(self, cls_name=None, method_name=None, *args, **kwargs):
        self.cls_name = cls_name
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return str(self.__dict__)


class Response(object):

    def __init__(self, data=None, err=None, code=200):
        self.data = data
        self.err = err
        self.code = code


class DeCoder(object):
    def decoder(self, d_bytes: bytes, cls):
        pass


class EnCoder(object):
    def encoder(self, o):
        pass


class JsonEnCoder(EnCoder):

    def encoder(self, o):
        if not o or not isinstance(o, object):
            raise TypeError('need an object')
        dicts = {item[0]: item[1] for item in filter(lambda item: not str(item[0]).startswith('_'), o.__dict__.items())}
        for key in dicts:
            if isinstance(dicts[key], (list, dict, tuple, str, int, float, bool)):
                continue
            elif dicts[key] and isinstance(dicts[key], object):
                dicts[key] = self.encoder(dicts[key])
        return bytes(json.dumps(dicts, ensure_ascii=False), encoding='utf8')


class JsonDeCoder(DeCoder):
    def decoder(self, d_bytes: bytes, cls):
        if not callable(cls):
            return TypeError('Decoder Need a class')
        try:
            data = json.loads(d_bytes.decode('utf8'))
            instance = cls()
            for item in data:
                setattr(instance, item, data[item])
            return instance
        except Exception as e:
            return None


class Client(object):

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._freeze = True
        self._observed = []
        self._live = True

    def is_free(self):
        return self._freeze

    def test(self):
        try:
            self.connect()
            self._freeze = False
            return True
        except Exception as e:
            self._live = False
            return False

    def send(self, data, *args, **kwargs):
        self._socket.send(data)
        self._socket.send(b'\0')

    def recv(self, buff_size):
        return self._socket.recv(buff_size)

    def close(self):
        try:
            self._socket.close()
        except Exception as e:
            pass
        finally:
            self._freeze = True

    def is_live(self):
        return self._live

    def connect(self):
        self._socket.connect((self._host, self._port))




