import os
import json
import logging as _logging


logging = _logging.getLogger(__name__)


def save(path, filename, data):
    file = os.path.join(path, filename)
    with open(file, "w+", encoding='utf8') as stream:
        stream.write(data)
        stream.close()


def load_json(*file):
    file = os.path.join(*file)
    try:
        data = None
        with open(file, "r", encoding="utf8") as file:
            data = json.loads(file.read())
            file.close()
        return data
    except json.decoder.JSONDecodeError as e:
        return None


def catcher(capture=True, default_value=None):
    def wrapper(invoke):
        def try_catch(*args, **kwargs):
            try:
                return invoke(*args, **kwargs)
            except Exception as e:
                _capture = kwargs.get("catcher_capture", capture)
                _default = kwargs.get("catcher_default", default_value)
                if _capture and callable(invoke):
                    logging.debug("%s:%s args: %s, kwargs: %s" % (invoke.__name__, str(e), str(args), str(kwargs)))
                    return _default
                raise e
        return try_catch
    return wrapper
