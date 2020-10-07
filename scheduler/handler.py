import threading
from .client import *

import logging as _logging

logging = _logging.getLogger(__name__)


def thread(name=None):
    def wrapper(func):
        def set_args(*s_args, **s_kwargs):
            if not callable(func):
                raise TypeError('not function')
            c_thread = threading.Thread(target=func, name=name, args=s_args, **s_kwargs)
            c_thread.start()
            return c_thread

        return set_args

    return wrapper


def remote_read(sock: socket.socket, buff_len=1024):
    try:
        data = b''
        while True:
            r_data = sock.recv(buff_len)
            if not r_data:
                break
            data += r_data
            if data[-2:] == b'\r\n':
                break
        return data[:-2]
    except Exception as e:
        return None


def remote_write(sock: socket.socket, data):
    try:
        sock.send(data)
        sock.send(b'\r\n')
    except Exception as e:
        logging.error("Write Error: {}".format(e), exc_info=e)


class Handler(object):

    def __init__(self):
        self._decoder = None
        self._encoder = None
        self._remote_success = None
        self._remote_failure = None

    def is_usable(self):
        """
        是否可用
        """
        pass

    def on_success(self, func):
        self._remote_success = func

    def on_failure(self, func):
        self._remote_failure = func

    def set_decoder(self, decoder: DeCoder):
        self._decoder = decoder

    def set_encoder(self, encoder: EnCoder):
        self._encoder = encoder

    def decoder(self) -> DeCoder:
        return self._decoder

    def encoder(self) -> EnCoder:
        return self._encoder

    def handler(self, data, async_handler=False):
        pass

    def register(self, clazz, instance_o):
        pass

    def on_connect(self, sock, addr):
        pass

    def on_close(self, sock):
        pass

    def on_error(self, sock):
        pass


class SocketHandler(Handler):

    def __init__(self, port, host):
        super().__init__()
        self.addr = (port, host)
        self._socket = None

    @staticmethod
    def _create_tcp():
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def is_usable(self):
        try:
            self._socket = self._create_tcp()
            self._socket.connect(self.addr)
            return True
        except Exception as e:
            return False

    async def _request(self, data):
        logging.debug("Remote Invoke {}".format(self._socket))
        remote_write(self._socket, data)
        data = remote_read(self._socket)
        response = self.decoder().decoder(data, Response)
        if response and response.code == 200:
            return response.data
        return None

    async def async_handler(self, data):
        result = await self._request(data)
        return result

    def handler(self, data, async_handler=False):
        data = self.encoder().encoder(data)
        if async_handler:
            try:
                self._request(data).send(None)
            except StopIteration as e:
                logging.info("Remote Response: {}".format(e))
                if self._remote_success:
                    self._remote_success(e.value)
            except Exception as e:
                logging.error("Handler Error", exc_info=e)
            finally:
                self._socket.close()


class RemoteClientHandler(Handler):
    class ServiceDescription(object):

        def __init__(self, instance, clazz_name, method_name):
            self.instance = instance
            self.clazz_name = clazz_name
            self.method_name = method_name

        def __repr__(self):
            return self.__str__()

        def __str__(self):
            return '{}::{}'.format(self.clazz_name, self.method_name)

        def invoke(self, *args, **kwargs):
            func = getattr(self.instance, self.method_name)
            if not callable(func):
                raise TypeError('not callable')
            return func(*args, **kwargs)

    def __init__(self, decoder: DeCoder, encoder: EnCoder):
        super().__init__()
        self._decoder = decoder
        self._encoder = encoder
        self._services = []

    def register(self, clazz, instance_o):
        if not callable(clazz) and not isinstance(instance_o, clazz):
            raise TypeError("Not class or Not instance clazz")
        services = [item[0] for item in
                    filter(lambda item: not str(item[0]).startswith('_') and callable(item[1]), clazz.__dict__.items())]
        for service in services:
            self._services.append(self.ServiceDescription(instance_o, clazz.__name__, service))

    def lock_up(self, clazz_name, method_name):
        for service in self._services:
            if service.clazz_name == clazz_name and service.method_name == method_name:
                return service
        return None

    def on_connect(self, sock: socket.socket, addr):
        logging.debug('Handler Remote Invoke: {}'.format(sock))
        request: Request = self._decoder.decoder(remote_read(sock), Request)
        logging.debug("Invoke Info: {}".format(request))
        service: RemoteClientHandler.ServiceDescription = self.lock_up(request.cls_name, request.method_name)
        response = None
        try:
            if request and service:
                data = service.invoke(*request.args, **request.kwargs)
                response = Response(data=data)
            data = self._encoder.encoder(response or Response(code=500))
            remote_write(sock, data)
        except Exception as e:
            logging.error(e, exc_info=e)
        finally:
            logging.info("Handler Finish: {}".format(request))
            sock.close()

    def on_close(self, sock):
        pass

    def on_error(self, sock):
        pass
