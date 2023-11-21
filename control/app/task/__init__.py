from pkgutil import iter_modules
from importlib import import_module
import logging


def generate_task_list():
    task_list = []

    for _, name, _ in iter_modules(__path__):
        logging.debug(f"task generate_task_list discovered {name}")
        mod = import_module(f"task.{name}")
        task_list += [mod.run]

    return task_list


global_task_list = generate_task_list()
