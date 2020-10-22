import socket
import logging
from .utils import run_thread


logger = logging.getLogger(__name__)



def get_local_host():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(('8.8.8.8', 80))
        return sock.getsockname()[0]
    finally:
        sock.close()
    

class Verify(object):

    def verify(self, connect):
        pass


class Connector(object):

    def __init__(self, sock):
        self._sock: socket.socket = sock
        self._verify = None
        peername = self._sock.getpeername()
        self._host = peername[0]
        self._port = peername[1]

    def set_verify(self, verify):
        if not isinstance(verify, Verify):
            raise TypeError("Need a Verify")
        self._verify = verify

    def available(self):
        if self._verify:
            return self._verify.verify(self)
        return True

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

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

    def __init__(self, host, port):
        self._host = host
        self._port = port

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

    def __init__(self, host, port, listen=10):
        super().__init__(host, port)
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
        logger.info("Handler client: {}:{}".format(addr[0], addr[1]))
        self._handler(DefaultConnector(sock), **kwargs)

    def start(self):
        logger.info("{}:{}".format(self.host, self.port))
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


def connect(host, port, connector_cls=None, timeout=30) -> Connector:
    sock = socket.create_connection((host, port), timeout=timeout)
    return DefaultConnector(sock) if not connector_cls and not callable(connector_cls) else connector_cls(sock)
