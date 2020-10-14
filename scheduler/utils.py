import multiprocessing
import threading
import logging


def check_type(o, o_type, err=None):
    if not isinstance(o, o_type):
        raise TypeError(
            err or "Except {} But Appred {}".format(str(o_type), str(o)))


def run_process(name=None, deamon=False):
    def wrapper(func):
        def set_args(*args, **kwargs):
            proc = multiprocessing.Process(target=func,
                                           daemon=deamon,
                                           name=name,
                                           args=args)
            proc.start()
            logging.debug("Process Started, pid: {}".format(proc.pid))
            return proc

        return set_args

    return wrapper


def run_thread(name=None):
    def wrapper(func):
        def set_args(*s_args, **s_kwargs):
            if not callable(func):
                raise TypeError('not function')
            c_thread = threading.Thread(
                target=func, name=name, args=s_args, **s_kwargs)
            c_thread.start()
            return c_thread

        return set_args

    return wrapper


def run_timer(interval=10, never=False):
    def wrapper(func):
        def set_args(*args, **kwargs):
            _interval = kwargs.get("interval", interval)
            _never = kwargs.get("never", never)
            print(_interval)
            try:
                del kwargs['interval']
                del kwargs['never']
            except Exception:
                pass
            _run = True
            while _never or _run:
                pt = threading.Timer(_interval, func, args=args, kwargs=kwargs)
                pt.start()
                pt.join()
                _run = False

        return set_args

    return wrapper


class TimerProcess(object):

    def __init__(self, target, interval=None, args=(), kwargs=None):
        self._interval = interval
        self._target = target
        self.args = args
        self.kwargs = kwargs
        multiprocessing.freeze_support()
        self._proc = multiprocessing.Process(
            target=self._proxy_invoke, args=args, kwargs=kwargs)
        self._result = multiprocessing.Queue()

    def _proxy_invoke(self, *args, **kwargs):
        try:
            logging.debug("wait process: {}, pid: {}".format(self._proc.name, self._proc.pid))
            res = self._target(*args, **kwargs)
            self._result.put(res)
        except Exception as e:
            self._result.put(e)

    def start_wait_result(self):
        try:
            self._proc.start()
            res = self._result.get(timeout=self._interval)
            if isinstance(res, Exception):
                raise ValueError(res.args)
            return res
        except ValueError as e:
            raise e
        except Exception:
            raise TimeoutError()
        finally:
            if self._proc.is_alive():
                self._proc.kill()


class PipeProcess(object):

    def __init__(self):
        self._pipe = multiprocessing.Pipe()
        self._proc = multiprocessing.Process(target=self._proc_target)

    def _proc_target(self, *args, **kwargs):
        pip = self._pipe[1]
        func = pip.recv()
        args = pip.recv()
        kwargs = pip.recv()
        func(*args,  **kwargs)

    def wait_for_value(self, func, args, kwargs):
        self._proc.start()
        pip = self._pipe[0]
        pip.send(func)
        pip.send(args)
        pip.send(kwargs)
        return self._pipe[0].recv()


def fun_proxy(func, *args, **kwargs):
    def proxy(*args, **kwargs):
        return func(*args, **kwargs)
    return proxy


def object_proxy(o):
    def proxy():
        return o

    return proxy


def make_timer_process(func, interval, args=(), kwargs=None):
    return TimerProcess(func, interval=interval, args=args, kwargs=dict(kwargs or {})).start_wait_result()
