import socket
import logging
from .utils import run_thread


logger = logging.getLogger(__name__)


class Connector(object):

    def __init__(self, sock):
        self._sock = sock

    @property
    def socket(self):
        return self._sock

    def write(self, data):
        pass

    def read(self, buff_size=10):
        pass

    def flush(self):
        pass

    def close(self):
        pass


class DefaultConnector(Connector):

    def __init__(self, sock):
        super().__init__(sock)

    def write(self, data):
        self.socket.write(data)

    def read(self, data):
        self.socket.read()

    def flush(self):
        self.socket.write("")

    def close(self):
        if self.socket != None:
            self.socket.close()
        self._sock = None


class Server(object):

    def __init__(self, host, port, handler):
        self._host = host
        self._port = port
        self._handler = handler

    @property
    def port(self):
        return self._port

    @property
    def host(self):
        return self._host

    def start(self):
        pass

    def stop(self):
        pass


class TcpServer(Server):

    def __init__(self, host, port, handler, listen=10):
        super().__init__(host, port, handler)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(listen)
        self._socket = sock

    @property
    def socket(self):
        return self._socket

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._socket != None:
            self.close()

    @run_thread()
    def _handler_client(self, sock, addr, *args, **kwargs):
        logging.info("Handler client: {}:{}".format(addr[0], addr[1]))
        self._handler(DefaultConnector(sock), **kwargs)

    def start(self):
        logging.info("{}:{}".format(self.host, self.port))
        while True:
            sock = self._socket.accept()
            self._handler_client(*list(sock))

    def stop(self):
        self._socket.close()
        self._socket = None


def listen_tcp(port, handler, host='0.0.0.0') -> Server:
    tcp_server = TcpServer(host, port, handler)
    tcp_server.start()
    return tcp_server


def connect(host, port, timeout=30) -> Connector:
    sock = socket.create_connection((host, port), timeout=timeout)
    return DefaultConnector(sock)

