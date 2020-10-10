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

