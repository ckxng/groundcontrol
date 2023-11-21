from pprint import pformat
from datetime import datetime
import logging
from control.global_config import global_config as cfg

S_STARTING = "starting"
S_RUNNING = "running"
S_STOPPING = "stopping"
S_SLEEPING = "sleeping"


class Status:
    status_file: str = cfg.status_file
    state: str = S_STARTING
    updated: str = None
    written: str = None
    auto_update_status_file: bool = True
    current_task: str = None

    def __init__(self):
        pass

    def set_auto_update_status_file(self, value: bool = True):
        self.auto_update_status_file = value
        return self

    def unset_auto_update_status_file(self):
        self.auto_update_status_file = False
        return self

    def set_updated(self, updated: str = None):
        if updated is None:
            self.updated = datetime.utcnow().isoformat()
        else:
            self.updated = updated
        return self

    def set_state(self, state: str):
        self.state = state
        self.set_updated()
        logging.debug(f"set_state={state}")
        if self.auto_update_status_file:
            self.update()
        return self

    def set_current_task(self, current_task: str):
        self.current_task = current_task
        self.set_updated()
        logging.debug(f"set_current_task={current_task}")
        if self.auto_update_status_file:
            self.update()
        return self

    def set_written(self, written: str = None):
        if written is None:
            self.written = datetime.utcnow().isoformat()
        else:
            self.written = written
        return self

    def update(self):
        self.set_written()
        with open(self.status_file, "w") as f:
            f.write(self.to_json())

    def to_json(self):
        return pformat({
                "state": self.state,
                "updated": self.updated,
                "written": self.written,
                "current_task": self.current_task,
            })