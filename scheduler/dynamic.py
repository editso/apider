import importlib
import logging
import os
import sys


def find_class(module, cls_or_tuple):
    clazz = []
    for item in module.__dir__():
        try:
            item = getattr(module, item)
            if issubclass(item, cls_or_tuple):
                clazz.append(item)
        except Exception:
            pass
    return clazz


def new_all_instalce(module, cls_or_tuple, *args, **kwargs):
    ins = []
    for clazz in module.__dir__():
        issub = False
        try:
            clazz = getattr(module, clazz)
            if issubclass(clazz, cls_or_tuple):
                issub = True
        except Exception:
            continue
        if issub:
            ins.append(clazz(*args, **kwargs))
    return ins


def load_module(module_name):
    sys.path.append(os.getcwd())
    if not module_name:
        raise ModuleNotFoundError(module_name)
    module = module_name.replace('.py', '')
    return importlib.import_module(module)
