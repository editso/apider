from queue import Queue
import time
from .utils import check_type, run_process, run_thread, run_timer, make_timer_process
import threading
import logging
from .net import connect, Connector, TcpServer
import socket
import json
import multiprocessing


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
        return self._queuq.get()


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

    @property
    def err() -> ErrorTask:
        return self._err_task

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

    def __init__(self, loop_collect_time: float = 30):
        self._loop_collect_time = 30
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
        return None

    def _task_error(self, error):
        self._failure_task.append(error)

    def _dispatch(self, dispatcher: Dispatcher, task_item):
        logging.debug("dispatche: {}".format(task_item))
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
        except Exception as e:
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

    def recv_from(self, r: (Request or Response)):
        data = self.read()
        return self.decode(data, r)

    def decode(self, data, r: (Request or Response)):
        return self._decoder.decoder(data, r)

    def encode(self, r: (Request or Response)):
        return self._encoder.encoder(r)


class RemoteInvokeDispatcher(Dispatcher):
    """
        远程调用派发器
    """

    def __init__(self, adapter=None):
        super().__init__()
        self._lock = threading.Lock()
        self._mutex = threading.Condition(self._lock)
        self._remote_task = []
        self._adapter = adapter
        self._loop_dispatch_task()
        self._listener = []

    @run_thread()
    def _remote_invoke(self, connector: RemoteInvokeConnector, request: Request):
        _connector = connector
        _request = request
        try:
            connector.send_from(_request)
            res = connector.recv_from(Response)
            if res is None or int(res.code) in [4004, 5000, 5001, 5002]:
                self._notify_all_litener("error", _request)
            elif int(res.code) == 200:
                self._notify_all_litener("success", res, _request)
        except Exception as e:
            logging.debug("Invoke Error", exc_info=e)
            self._notify_all_litener("error", _request)
        finally:
            try:
                connector.close()
            except Exception:
                pass
            self._adapter.finish(_connector, _request)

    def _select_connector(self) -> RemoteInvokeConnector:
        remote_invoke = None
        while not remote_invoke:
            try:
                remote_invoke = self._adapter.get()
                if not remote_invoke or not isinstance(remote_invoke, RemoteInvokeConnector):
                    continue
            except Exception as e:
                logging.error(
                    "RemoteInvokeConnector An exception occurs", exc_info=e)
                continue
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
            task = self._pop_task()
            connect = self._select_connector()
            self._remote_invoke(connect, task)

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
                self._mutex.wait(10)  # 等待10秒,防止僵尸
        return True

    def add_listener(self, listener):
        check_type(listener, DispatchListener)
        self._listener.append(listener)

    def _notify(self):
        with self._mutex:
            self._mutex.notify()

    def _notify_all_litener(self, name, *args, **kwargs):
        for item in self._listener:
            if name == 'error':
                item.error(*args, **kwargs)
            elif name == 'success':
                item.success(*args, **kwargs)

    def dispatch(self, task_item):
        self._remote_task.append(task_item)
        self._notify()


class InvokeService(object):

    def __init__(self, cls_name, method_name, func=None, instance=None):
        self._cls_name = cls_name
        self._method_name = method_name
        self._instance = instance
        self._func = func

    @property
    def cls_name(self):
        return self._cls_name

    @property
    def method_name(self):
        return self._method_name

    def invoke(self, *args, **kwargs):
        try:
            return self._func(self._instance, *args, **kwargs)
        except Exception as e:
            logging.debug("Service Invoke error", exc_info=e)
            return None

    def __repr__(self):
        return "<{}#{}>".format(self.cls_name, self.method_name)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, obj):
        if not isinstance(obj, InvokeService):
            return False
        return obj.method_name == self.method_name and obj.cls_name == self.cls_name


class RemoteInvokeServer(TcpServer):
    """远程调用服务端
    """

    def __init__(self, host, port, invoke_timeout=None, *args, **kwargs):
        super().__init__(host, port)
        self._service = []
        self._invoke_timeout = invoke_timeout

    def lock_up(self, cls_name, method_name):
        for item in self._service:
            if item.method_name == method_name and item.cls_name == cls_name:
                return item
        return None

    def add_service(self, instance):
        check_type(instance, object)
        name = instance.__class__.__name__
        for k, v in instance.__class__.__dict__.items():
            if not callable(v) or k.startswith('_'):
                continue
            service = InvokeService(name, k, v, instance)
            if service not in self._service:
                self._service.append(service)

    @run_thread()
    def handler_remote_invoke(self, conner: RemoteInvokeConnector):
        req: Request = None
        try:
            resp = None
            req = conner.recv_from(Request)
            service = None
            result = None
            if not req:
                resp = make_response(message="请求有误", code=5000)
            else:
                service = self.lock_up(req.cls_name, req.method_name)
            if service:
                result = make_timer_process(
                    service.invoke, interval=req.timeout or self._invoke_timeout, args=req.args, kwargs=req.kwargs)
            elif not resp:
                resp = make_response(data=req, message="找不到服务", code=4004)
            if isinstance(result, Response):
                resp = result
            else:
                resp = make_response(data=result)
            conner.send_from(resp)
        except TimeoutError:
            logging.debug("Invoke timeout")
            return make_response(data=req, message="调用超时", code=5002)
        except Exception as e:
            logging.debug("Invoke error: {}".format(e), exc_info=e)
            return make_response(data=None, message="未知错误", code=5001)
        finally:
            conner.close()

    def start(self):
        logging.info("Started Server, {}:{}".format(self.host, self.port))
        while True:
            sock, addr = self.socket.accept()
            remote_conner = RemoteInvokeConnector(sock)
            self.handler_remote_invoke(remote_conner)


def remote_invoke_dispatcher(adapter: ConnectorAdapter):
    check_type(adapter, ConnectorAdapter)
    return RemoteInvokeDispatcher(adapter)


def make_request(cls_name, method_name, timeout=None, *args, **kwargs):
    return Request(cls_name, method_name, timeout=timeout, *args, **kwargs)


def make_response(data=None, message=None, code=200):
    return Response(code=code, message=message, data=data)
