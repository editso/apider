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

    def __init__(self, target, interval=None,  args=(), kwargs={}):
        self._interval = interval
        self._target = target
        self.args = args
        self.kwargs = kwargs
        self._proc = multiprocessing.Process(
            target=self._proxy_invoke, args=args, kwargs=kwargs)
        self._result = multiprocessing.Queue()

    def _proxy_invoke(self, *args, **kwargs):
        res = self._target(*args, **kwargs)
        self._result.put(res)

    def start_wait_result(self):
        self._proc.start()
        try:
            logging.debug("wait process: {}, pid: {}".format(self._proc.name, self._proc.pid))
            return self._result.get(timeout=self._interval)
        except Exception:
            raise TimeoutError
        finally:
            if self._proc.is_alive():
                self._proc.kill()

def make_timer_process(target, interval, args=(), kwargs={}):
    return TimerProcess(target, interval=interval, args=args, kwargs=kwargs).start_wait_result()
