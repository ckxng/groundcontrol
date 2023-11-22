import logging
from os import getenv, path


class Config:
    logging_level: int = logging.INFO
    logging_file: str = "/run/log.txt"
    status_file: str = "/run/status.txt"
    tank_database_name: str = "postgres"
    tank_database_user: str = "postgres"
    tank_database_password: str = "postgres"
    tank_database_hostname: str = "tank"
    run_interval: int = 1200

    def __init__(self):
        pass

    def update_from_environ(self):
        self.set_logging_file()
        self.set_logging_level()
        self.set_status_file()
        self.set_tank_database_name()
        self.set_tank_database_user()
        self.set_tank_database_password()
        self.set_tank_database_hostname()
        self.set_run_interval()

    def set_logging_level(self, logging_level: str = None):
        if logging_level is None:
            logging_level = getenv("CONTROL_LOGGING_LEVEL")
        if logging_level is not None:
            ll = logging_level.upper()
            if ll == "DEBUG":
                self.logging_level = logging.DEBUG
            elif ll == "INFO":
                self.logging_level = logging.INFO
            elif ll == "CRITICAL":
                self.logging_level = logging.CRITICAL
            elif ll == "WARN" or ll == "WARNING":
                self.logging_level = logging.WARNING
            elif ll == "ERROR":
                self.logging_level = logging.ERROR
            elif ll == "FATAL":
                self.logging_level = logging.FATAL

    def set_logging_file(self, logging_file: str = None):
        if logging_file is None:
            logging_file = getenv("CONTROL_LOGGING_FILE")
        if logging_file is not None and path.exists(logging_file):
            self.logging_file = logging_file

    def set_status_file(self, status_file: str = None):
        if status_file is None:
            status_file = getenv("CONTROL_STATUS_FILE")
        if status_file is not None and path.exists(status_file):
            self.status_file = status_file

    def set_tank_database_name(self, tank_database_name: str = None):
        if tank_database_name is None:
            self.tank_database_name = getenv("POSTGRES_DB")
        else:
            self.tank_database_name = tank_database_name

    def set_tank_database_user(self, tank_database_user: str = None):
        if tank_database_user is None:
            self.tank_database_user = getenv("POSTGRES_USER")
        else:
            self.tank_database_user = tank_database_user

    def set_tank_database_password(self, tank_database_password: str = None):
        if tank_database_password is None:
            self.tank_database_password = getenv("POSTGRES_PASSWORD")
        else:
            self.tank_database_password = tank_database_password

    def set_tank_database_hostname(self, tank_database_hostname: str = None):
        if tank_database_hostname is None:
            self.tank_database_hostname = getenv("TANK_HOSTNAME")
        else:
            self.tank_database_hostname = tank_database_hostname

    def set_run_interval(self, run_interval: int = 60):
        if run_interval is None:
            run_interval = int(getenv("CONTROL_RUN_INTERVAL"))
        self.run_interval = run_interval
