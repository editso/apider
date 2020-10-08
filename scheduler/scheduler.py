from queue import Queue
import logging as _logging
from multiprocessing import Process
from threading import Thread
from .handler import *
import time

logging = _logging.getLogger(__name__)


class Scheduler(object):

    def get_instance(self, cls):
        pass

    def register(self, host, port):
        pass

    def stop(self):
        exit(0)

    def add_task(self, task):
        pass

    def execute_task(self):
        pass


class TaskInfo(object):

    def __init__(self, class_name, method_name, *args, **kwargs):
        self._class_name = class_name
        self._name = method_name
        self._args = args
        self._kwargs = kwargs

    def get_cls_name(self):
        return self._class_name

    def get_name(self):
        return self._name

    def get_args(self):
        return self._args

    def get_kwargs(self):
        return self._kwargs


class Task(object):
    def re_task(self, task_meta: TaskInfo):
        pass

    def has_task(self):
        """
        有任务返回真
        """
        pass

    def get_task(self) -> TaskInfo:
        """
        返回当前需要被处理的任务
        """
        pass


class ProxyHandler(object):

    def __init__(self, target):
        self._target = target

    def invoke(self, proxy, method, *args, **kwargs):
        pass


class Worker(object):

    def __init__(self):
        self._target = None
        self._task_meta = None
        self._success = None
        self._failure = None

    def is_available(self):
        """
        是否可工作
        """
        pass

    def is_working(self):
        pass

    def get_target(self) -> Task:
        return self._target

    def set_target(self, task: Task):
        self._target = task

    def start_working(self, task_info: TaskInfo):
        self._task_meta = task_info

    def _clear(self):
        self._target = None
        self._task_meta = None

    def on_failure(self, func):
        """
        失败回调
        """
        self._failure = func

    def on_success(self, func):
        """
        成功回调
        """
        self._success = func


class RemoteScheduler(Scheduler):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, decoder: DeCoder, encoder: EnCoder):
        self._workers = []  # 空闲的
        self._workings = []  # 忙碌的
        self._error_worker = []  # 错误的
        self._tasks = Queue()
        self._current_task = None
        self._decoder = decoder
        self._encoder = encoder
        self._is_task_running = False
        self._task_process = None

    def get_instance(self, cls):
        if not callable(cls):
            raise TypeError('not a class')
        return RemoteProxyHandler(cls, self)

    def _get_task(self):
        if not self._current_task and self._tasks.qsize() > 0:
            self._current_task = self._tasks.get()
        if not self._current_task:
            return
        if not self._current_task.has_task():
            self._current_task = None
            return self._get_task()
        return self._current_task, self._current_task.get_task()

    def _worker_on_failure(self, worker: Worker, task: Task, task_meta, *args, **kwargs):
        logging.info('Worker Failure: {}'.format(worker))
        if worker.is_available():
            self._workers.append(worker)
            self._workings.remove(worker)
            task.re_task(task_meta)
        else:
            self._workings.remove(worker)
            self._error_worker.append(worker)
            

    def _worker_on_success(self, worker, *args, **kwargs):
        logging.debug("Worker Finish: {}".format(worker))
        try:
            self._workers.append(worker)
            self._workings.remove(worker)
        except Exception:
            pass

    def register(self, host, port):
        socket_handler = SocketHandler(host, port)
        socket_handler.set_decoder(self._decoder)
        socket_handler.set_encoder(self._encoder)
        worker = RemoteWorker(socket_handler)
        worker.on_failure(self._worker_on_failure)
        worker.on_success(self._worker_on_success)
        self._workers.append(worker)

    def invoke(self, request):
        if not isinstance(request, Request):
            raise TypeError('Need a Request object')
        data = self._encoder.encoder(request)
        remote_service: Client = self._select_worker()
        if not remote_service:
            raise ConnectionError('Remote connect failure')
        try:
            remote_service.send(data)
            r_data: bytes = b''
            while True:
                recv = remote_service.recv(1024)
                if not recv:
                    break
                r_data += recv
            res = self._decoder.decoder(r_data, Response)
            if res and res.code == 200:
                return res.data
            return None
        finally:
            remote_service.close()

    def add_task(self, task: Task):
        self._tasks.put(task)

    def _select_worker(self):
        """
        选择工作器
        """
        worker = None
        remove_worker = []
        for worker in self._workers:
            if worker.is_available():
                remove_worker.append(worker)
                self._workings.append(worker)
                break
            else:
                remove_worker.append(worker)
                self._error_worker.append(worker)
                worker = None
        for err in remove_worker:
            self._workers.remove(err)
        return worker

    def _lock_task(self):
        self._is_task_running = True

    def _unlock_task(self):
        self._is_task_running = False

    def _start_task(self):
        logging.debug('Task Running')
        task, task_meta = self._get_task()
        running_tasks = []
        while task_meta:
            if not isinstance(task_meta,  TaskInfo):
                continue
            logging.debug('Handler Task By {}'.format(task_meta))
            worker: Worker = self._select_worker()
            while not worker:
                worker = self._select_worker()
                time.sleep(1)
            worker.set_target(task)
            run_thread = Thread(
                target=lambda: worker.start_working(task_meta))
            running_tasks.append(run_thread)
            run_thread.start()
            task, task_meta = self._get_task() or (None, None)
        for task in running_tasks:
            task.join()  # 等待任务完成
        logging.info("Scheduler Task Success")
        self._kill_process()

    def _kill_process(self):
        self._task_process.close()
        self._task_process = None
        self._unlock_task()

    def execute_task(self):
        self._lock_task()
        self._task_process = Process(target=self._start_task)
        self._task_process.start()


class RemoteWorker(Worker):

    def __init__(self, handler: Handler):
        super().__init__()
        self._handler = handler
        self._task_info = None

    def is_available(self):
        return self._handler.is_usable()

    def _on_success(self, *args, **kwargs):
        if self._success:
            self._success(self, self.get_target(), self._task_meta)

    def _on_failure(self, *args, **kwargs):
        if self._failure:
            self._failure(self, self.get_target(), self._task_meta)

    def start_working(self, task_info: TaskInfo):
        super().start_working(task_info)
        logging.debug('Working Start')
        self._handler.on_success(self._on_success)
        self._handler.on_failure(self._on_failure)
        self._handler.handler(
            Request(*task_info.get_args(),
                    method_name=task_info.get_name(),
                    cls_name=task_info.get_cls_name(),
                    **task_info.get_kwargs()),
            async_handler=True)


class RemoteProxyHandler(ProxyHandler):

    def __init__(self, target, scheduler):
        super().__init__(target)
        if not isinstance(scheduler, RemoteScheduler):
            raise TypeError('not instance Scheduler')
        self._scheduler = scheduler

    def __getattr__(self, item):
        func = self._target.__dict__.get(item)
        if not callable(func):
            return TypeError('remote error')
        return self._wrapper(func)

    def _wrapper(self, func):
        def set_args(*args, **kwargs):
            return self.invoke(self._target, func, *args, **kwargs)
        return set_args

    def invoke(self, proxy, method, *args, **kwargs):
        request = Request(proxy.__name__, method.__name__, *args, **kwargs)
        return self._scheduler.invoke(request)
