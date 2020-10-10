import socket
import threading
from .handler import *
from .utils import thread
import logging


logging = logging.getLogger('server')


class Server(object):

    def __init__(self, host, port, auth=None, handler=None):
        if not isinstance(handler, Handler):
            raise TypeError('handler error')
        self._host = host
        self._port = port
        self._auth = auth
        self._handler = handler
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, port))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @thread()
    def start(self):
        self._socket.listen(10)
        logging.info('listen: {}:{}'.format(self._host, self._port))
        while True:
            self.handler(*self._socket.accept())

    @thread()
    def handler(self, sock, addr):
        self._handler.on_connect(sock, addr)


class RemoteServer(Server):

    def __init__(self, handler, *args, **kwargs):
        super().__init__(*args, handler=handler, **kwargs)
        if not isinstance(handler, RemoteClientHandler):
            raise TypeError('Remote handler error')
            
