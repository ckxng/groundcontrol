import logging
from control.task_return import TR_OK


def run():
    logging.debug("task hello run executing")
    return TR_OK, {}

