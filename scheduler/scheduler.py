from queue import Queue
import time
from .utils import check_type, run_process, run_thread, \
    run_timer, make_timer_process, \
    object_proxy, PipeProcess, md5_hex_digest
import threading
import logging
from .net import connect, Connector, TcpServer, Verify
import multiprocessing
import elasticsearch
import socket
import json


class Task(object):

    def has_task(self):
        return False

    def next_task(self):
        pass


class ErrorTask(Task):

    def __init__(self):
        self._queue = Queue()

    def push(self, task):
        self._queue.put(task)

    def has_task(self):
        return self._queue.qsize()

    def get_task(self):
        return self._queue.get()


class Request(object):
    cls_name = None
    method_name = None
    args = None
    kwargs = None
    timeout = None

    def __init__(self, cls_name=None, method_name=None, timeout=None, *args, **kwargs):
        self.cls_name = cls_name
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs
        self.timeout = timeout

    def __repr__(self):
        return str(self.__dict__)


class Response(object):

    code = None

    message = None

    data = None

    def __init__(self, code=None, message=None, data=None):
        self.code = code
        self.message = message
        self.data = data

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return self.__repr__()


class Reporter(object):
    """汇报器"""

    def set_date(self, date):
        pass

    def set_title(self, title):
        pass

    def set_body(self, body):
        pass

    def report(self):
        """汇报信息"""


class SMTPReporter(Reporter):

    def __init__(self, sender, receives: []):
        super().__init__()
        self._sender = sender
        self._receives = receives

    def set_body(self, body):
        pass

    def set_title(self, title):
        return super().set_title(title)

    def set_date(self, date):
        return super().set_date(date)

    def report(self):
        return super().report()


class DispatchListener(object):

    def error(self, *args, **kwargs):
        """任务失败调用"""

    def success(self, *args, **kwargs):
        """任务成功调用"""


class Dispatcher(object):

    def can_handle(self, task_item: Task):
        pass

    def add_listener(self, listener: DispatchListener):
        pass

    def dispatch(self, task_item):
        """派发任务
        """


class TaskQueue(object):

    def __init__(self):
        self._queue = Queue()
        self._cur_task: Task = None

    def pop(self) -> Task:
        task_item = None
        while task_item is None:
            try:
                if not self._cur_task:
                    self._cur_task = self._queue.get()
                if not self._cur_task.has_task():
                    self._cur_task = None
                    continue
                task_item = self._cur_task.next_task()
            except Exception as e:
                logging.debug("Get Task Error", exc_info=e)
        return task_item

    def push(self, task: Task):
        check_type(task, Task)
        self._queue.put(task)


class Scheduler(object):

    def __init__(self):
        self._dispatcher = []
        self._task = TaskQueue()
        self._failure_task = []

    def add_dispatcher(self, dispatcher):
        """为调度器添加一个分配器
        """
        check_type(dispatcher, Dispatcher)
        self._dispatcher.append(dispatcher)

    def register(self, task: Task):
        """注册一个任务
        """
        self._task.push(task)

    def _select_dispatch(self, task) -> Dispatcher:
        for dispatcher in self._dispatcher:
            if dispatcher.can_handle(task):
                return dispatcher

    def _task_error(self, error):
        self._failure_task.append(error)

    def _dispatch(self, dispatcher: Dispatcher, task_item):
        logging.debug("dispatch: {}".format(task_item))
        try:
            dispatcher.dispatch(task_item)
        except Exception as e:
            logging.debug("Dispatch Task Error: {}".format(e))
            self._task_error(self.Error(
                task_item, description="Dispatch Task Error", stack=e))

    def dispatch(self):
        """开始派发任务
        """
        while True:
            task_item = self._task.pop()
            print(task_item)
            dispatch = self._select_dispatch(task_item)
            if dispatch is None:
                error = self.Error(
                    task=task_item, description="Not Found Dispatcher")
                self._task_error(error)
                continue
            self._dispatch(dispatch, task_item)

    class Error(object):
        description = None
        code = None
        task = None
        stack = None

        def __init__(self, task, description=None, stack=None, code=500):
            self.description = description
            self.code = code
            self.task = task


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
        dicts = {item[0]: item[1] for item in filter(
            lambda item: not str(item[0]).startswith('_'), o.__dict__.items())}
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
        except Exception:
            return None


class ConnectorAdapter(object):

    def has_connector(self):
        return False

    def get(self) -> Connector:
        pass

    def finish(self, connector: Connector, task_item):
        pass


class RemoteInvokeConnector(Connector):

    def __init__(self, sock, decoder: DeCoder = JsonDeCoder(), encoder: EnCoder = JsonEnCoder()):
        super().__init__(sock)
        check_type(decoder, DeCoder)
        check_type(encoder, EnCoder)
        self._decoder: DeCoder = decoder
        self._encoder: EnCoder = encoder

    def read(self, buff_size=1024):
        data = b''
        while True:
            recv_data = self.socket.recv(buff_size)
            if not recv_data:
                return data
            data += recv_data
            if data[-2:] == b'\r\n':
                return data[0:-2]

    def write(self, data):
        try:
            self.socket.sendall(data)
            self.flush()
            return True
        except Exception as e:
            return False

    def flush(self):
        self.socket.send(b'\r\n')

    def close(self):
        try:
            self.socket.close()
        except Exception:
            pass

    def send_from(self, r: (Request or Response)):
        data = self.encode(r)
        logging.debug("send: {}".format(data))
        if not self.write(data):
            return False
        return True

    def __repr__(self):
        return "<{}@{}:{}>".format(self.__class__.__name__, self.host, self.port)

    def recv_from(self, r: (Request or Response))->object:
        data = self.read()
        return self.decode(data, r)

    def decode(self, data, r: (Request or Response)):
        return self._decoder.decoder(data, r)

    def encode(self, r: (Request or Response)) -> (Request or Response):
        return self._encoder.encoder(r)


class RemoteInvokeListener(object):

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_invoke(self, request: Request, connector: RemoteInvokeConnector):
        pass

    def on_invoke_finish(self, resp):
        pass

    def on_have(self, server):
        """可以继续接受处理时通知"""

    def on_full(self, server):
        """处理数量到达最大处理数时通知"""


class RemoteInvokeDispatcher(Dispatcher):
    """
        远程调用派发器
    """

    def __init__(self, adapter=None):
        super().__init__()
        self._lock = threading.Lock()
        self._mutex = threading.Condition(self._lock)
        self._remote_task = []
        self._adapter: ConnectorAdapter = adapter
        self._loop_dispatch_task()
        self._listener = []

    @run_thread()
    def _remote_invoke(self, connector: RemoteInvokeConnector, request: Request):
        _connector = connector
        _request = request
        try:
            connector.socket.settimeout(None)
            connector.send_from(_request)
            res = connector.recv_from(Response)
            logging.debug('resp: {}'.format(res))
            if res is None or int(res.code) in [4004, 5000, 5001, 5002]:
                self._notify_all_listener("error", _request)
            elif int(res.code) == 200:
                self._notify_all_listener("success", res, _request)
        except Exception as e:
            logging.debug("Invoke Error", exc_info=e)
            self._notify_all_listener("error", _request)
        finally:
            try:
                connector.close()
            except Exception:
                pass
            try:
                self._adapter.finish(_connector, _request)
            except Exception:
                pass

    def _select_connector(self) -> RemoteInvokeConnector:
        remote_invoke = None
        while not remote_invoke:
            try:
                remote_invoke = self._adapter.get()
                if not remote_invoke or not isinstance(remote_invoke, RemoteInvokeConnector):
                    continue
                break
            except Exception as e:
                logging.error(
                    "RemoteInvokeConnector An exception occurs", exc_info=e)
        return remote_invoke

    def _pop_task(self):
        with self._mutex:
            if len(self._remote_task) <= 0:
                self._mutex.wait()
            try:
                return self._remote_task.pop(0)
            except Exception:
                return self._pop_task()

    @run_thread()
    def _loop_dispatch_task(self):
        while True:
            try:
                task = self._pop_task()
                connect = self._select_connector()
                self._remote_invoke(connect, task)
            except Exception:
                pass

    def can_handle(self, task_item):
        if not isinstance(task_item, Request):
            return False
        while True:
            try:
                if self._adapter.has_connector():
                    return True
            except Exception as e:
                logging.debug("Connector Error:{}".format(e), exc_info=e)
            with self._mutex:
                self._mutex.wait(5)

    def add_listener(self, listener):
        check_type(listener, DispatchListener)
        self._listener.append(listener)

    def _notify(self):
        with self._mutex:
            self._mutex.notify()

    def _notify_all_listener(self, name, *args, **kwargs):
        for item in self._listener:
            if name == 'error':
                item.error(*args, **kwargs)
            elif name == 'success':
                item.success(*args, **kwargs)

    def dispatch(self, task_item):
        self._remote_task.append(task_item)
        self._notify()


class InvokeService(object):

    def __init__(self, cls, cls_name, method_name, func=None):
        self._cls_name = cls_name
        self._method_name = method_name
        self._func = func
        self._cls = cls

    @property
    def cls_name(self):
        return self._cls_name

    @property
    def method_name(self):
        return self._method_name

    def invoke(self, *args, **kwargs):
        instance: RemoteService = self._cls().__call__()
        return getattr(instance, self.method_name)(*args, **kwargs)

    def __repr__(self):
        return "<{}#{}>".format(self.cls_name, self.method_name)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, obj):
        if not isinstance(obj, InvokeService):
            return False
        return obj.method_name == self.method_name and obj.cls_name == self.cls_name


class RemoteInvokeServer(TcpServer, Verify):
    """远程调用服务端
    """

    def __init__(self, host, port, max_connection=1, invoke_timeout=None, *args, **kwargs):
        """
            invoke_timeout: 默认服务调用超时时间,
                            为空表示直到服务运行到结束
                            如果调用方设置了timeout那么此超时时间将会失效
        """
        super().__init__(host, port)
        self._service = []
        self._invoke_timeout = invoke_timeout
        self._max_connection = max_connection
        self._connectors = []
        self._listeners = []
        self._full = False

    def lock_up(self, cls_name, method_name):
        for item in self._service:
            if item.method_name == method_name and item.cls_name == cls_name:
                return item
        return None

    def add_service(self, cls):
        if not callable(cls) or not issubclass(cls, RemoteService):
            raise TypeError()
        name = cls.__name__
        for k, v in cls.__dict__.items():
            if not callable(v) or k.startswith('_'):
                continue
            service = InvokeService(cls, name, k, v)
            if service not in self._service:
                self._service.append(service)

    def add_listener(self, listener):
        if not isinstance(listener, RemoteInvokeListener):
            raise TypeError('Need a RemoteInvokeListener')
        self._listeners.append(listener)

    def verify(self, connector: Connector):
        code = None
        message = None
        try:
            data = connector.read()
            data = json.loads(data.decode('utf-8'))
            if data['type'] != 'verify':
                code = 400
                message = 'params error'
            else:
                if len(self._connectors) >= self._max_connection:
                    code = 400
                    message = 'not allow connect'
                else:
                    self.__append_running_server(connector)
                    code = 200
                    message = 'success'
                    return True
        except Exception:
            pass
        finally:
            try:
                connector.write(bytes(json.dumps({
                    'code': code or 500,
                    'message': message or 'no message'
                }, ensure_ascii=False), encoding='utf-8'))
            except Exception as e:
                pass
        return False

    def __append_running_server(self, server):
        self._connectors.append(server)
        if len(self._connectors) >= self._max_connection and not self._full:
            self._full = True
            self._notify_listener('on_full', self)

    def __recycle_running_server(self, server):
        self._connectors.remove(server)
        if self._full:
            self._full = False
            self._notify_listener('on_have', self)

    @run_thread()
    def handler_remote_invoke(self, conner: RemoteInvokeConnector):
        req = None
        resp = None
        try:
            resp = None
            req = conner.recv_from(Request)
            service: InvokeService = None
            result = None
            if not req:
                resp = make_response(message="请求有误", code=5000)
            else:
                service = self.lock_up(req.cls_name, req.method_name)
            if service:
                result = make_timer_process(
                    service.invoke,
                    interval=req.timeout or self._invoke_timeout,
                    args=req.args,
                    kwargs=req.kwargs)
                self._notify_listener('on_invoke', req, conner)
            elif req and not service:
                resp = make_response(
                    data=req.__dict__, message="找不到服务", code=4004)
            if isinstance(result, Response):
                resp = result
            elif not resp:
                resp = make_response(data=result)
        except TimeoutError as e:
            logging.debug("Invoke timeout", exc_info=e)
            resp = make_response(data=req.__dict__, message="调用超时", code=5002)
        except Exception as e:
            logging.debug("Invoke error: {}".format(e), exc_info=e)
            resp = make_response(data=None, message="未知错误", code=5001)
        finally:
            try:
                self.__recycle_running_server(conner)
            except Exception:
                pass
            try:
                conner.send_from(resp)
            except Exception:
                pass
            try:
                conner.close()
            except Exception:
                pass
            self._notify_listener('on_invoke_finish', resp)

    def _notify_listener(self, func_name, *args, **kwargs):
        for item in self._listeners:
            try:
                func = getattr(item, func_name)
                if not callable(func):
                    continue
                func(*args, **kwargs)
            except Exception as e:
                logging.debug('notify exception', exc_info=e)

    def stop(self):
        super().stop()
        self._notify_listener('on_stop')

    def start(self):
        logging.info("Started Server, {}:{}, max connect: {}".format(
            self.host, self.port, self._max_connection))
        self._notify_listener('on_start')
        self._notify_listener('on_have', self)  
        while True:
            sock, addr = self.socket.accept()
            remote_conner = RemoteInvokeConnector(sock)
            remote_conner.set_verify(self)
            if not remote_conner.available():
                try:
                    remote_conner.close()
                except Exception:
                    pass
                continue
            try:
                self.handler_remote_invoke(remote_conner)
            except Exception:
                pass


class ConnectionVerify(Verify):

    def verify(self, connector: RemoteInvokeConnector):
        try:
            if not connector.write(bytes(json.dumps({'type': 'verify'}, ensure_ascii=False), encoding='utf8')):
                return False
            data = connector.read()
            resp = json.loads(data)
            if resp['code'] == 200:
                return True
            return False
        except Exception as e:
            pass
        return False


class RemoteService(object):

    def __call__(self, *args, **kwargs):
        return self


class Poller(object):

    def __init__(self, target):
        self._queue = multiprocessing.Queue()
        self._target = target
        self._stop = False

    def _notify(self, data):
        self._target(data)

    def stop(self):
        self._stop = True

    def put(self, data):
        self._queue.put(data)

    @run_process()
    def poll(self):
        while True and not self._stop:
            try:
                data = self._queue.get(10)
                self._notify(data)
            except Exception:
                pass


class RemoteServer(object):
    """
    远程服务器
    """

    def pull(self, name=None, mark=None)->tuple:
        pass

    def push(self, host, port, mark=None, verify=None):
        pass

    def remove(self, name):
        pass


class ElasticRemoteServer(RemoteServer):

    __mappings__ = {
        "mappings": {
            "properties": {
                "host": {
                    "type": "text",
                    "index": False
                },
                "port": {
                    "type": "integer",
                    "index": False
                },
                "mark": {
                    "type": "text"
                },
                "verify": {
                    "type": "object",
                }
            }
        }
    }

    def __init__(self, es, index_name=None, verify_certs=False, **kwargs):
        self._es = elasticsearch.Elasticsearch(
            es,
            ssl_show_warn=None,
            verify_certs=verify_certs,
            **kwargs)
        self._index_name = index_name or 'remote_servers'
        self.init()

    def init(self):
        if not self._es.indices.exists(self._index_name):
            self._es.indices.create(self._index_name, body=self.__mappings__)

    def pull(self, name=None, mark=None):
        query = None
        if isinstance(mark, str):
            mark = [mark]
        if mark:
            query = {
                'query': {
                    "terms": {
                        'mark': mark
                    }
                }
            }
        data = self._es.search(index=self._index_name, body=query)[
            'hits']['hits']
        if data:
            return data[0]['_source']
        return None

    def push(self, host, port, mark=None, verify=None):
        e_id = md5_hex_digest("{}:{}".format(host, port))
        body = {
            'host': host,
            "port": port,
            'mark': mark or 'none',
            'verify': verify or {}
        }
        if not self._es.exists(self._index_name, e_id):
            self._es.index(self._index_name, body=body,
                           id=e_id, refresh='wait_for')
        else:
            self._es.update(self._index_name, e_id, {
                'doc': body
            }, refresh='wait_for')

    def remove(self, name):
        return super().remove(name)


class RemoteServerAdapter(ConnectorAdapter, Verify):

    def __init__(self, remote_server, timeout=10):
        self._lock = threading.Lock()
        self._mutex = threading.Condition(self._lock)
        self._remote: RemoteServer = remote_server
        self._timeout = timeout
        self._queue = Queue()

    def verify(self, connector: RemoteInvokeConnector):
        try:
            if not connector.write(bytes(json.dumps({'type': 'verify'}, ensure_ascii=False), encoding='utf8')):
                return False
            data = connector.read()
            resp = json.loads(data)
            if resp['code'] == 200:
                return True
            return False
        except Exception as e:
            pass
        return False

    def wait(self):
        if self._queue.empty():
            with self._mutex:
                self._mutex.wait(timeout=self._timeout)

    def has_connector(self):
        while True:
            try:
                server = self._remote.pull(mark=['none', 'wait'])
                if server is not None:
                    host, port = server
                    sock = socket.create_connection(
                        (host, port), timeout=self._timeout)
                    connector = RemoteInvokeConnector(sock)
                    connector.set_verify(self)
                    if connector.available():
                        self._queue.put(connector)
                        break
                    else:
                        connector.close()
            except Exception as e:
                logging.debug("connect error", exc_info=e)
            self.wait()
        return not self._queue.empty()

    def get(self):
        with self._mutex:
            return self._queue.get()

    def finish(self, connector: Connector, task_item):
        with self._mutex:
            self._remote.push(connector.host, connector.port)


def remote_connector_adapter(remote_server, timeout=10):
    return RemoteServerAdapter(remote_server, timeout=timeout)


def remote_invoke_dispatcher(adapter: ConnectorAdapter):
    check_type(adapter, ConnectorAdapter)
    return RemoteInvokeDispatcher(adapter)


def make_request(cls_name, method_name, timeout=None, *args, **kwargs):
    return Request(cls_name, method_name, timeout=timeout, *args, **kwargs)


def make_response(data=None, message=None, code=200):
    return Response(code=code, message=message, data=data)
