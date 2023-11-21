from psycopg2 import connect, OperationalError
from json import dumps
from control.global_config import global_config as cfg
from control.task_return import TR_OK
import logging


class TankNotAvailableError(Exception):
    pass


class TankInvalidSchemaError(Exception):
    pass


class Tank:
    __conn = None
    __schema_version: int = 0
    __MINIMUM_ALLOWED_SCHEMA_VERSION: int = 1

    def __init__(self,
                 database: str = cfg.tank_database_name,
                 user: str = cfg.tank_database_user,
                 host: str = cfg.tank_database_hostname,
                 password: str = cfg.tank_database_password):
        try:
            self.__connect(database=database,
                           user=user,
                           host=host,
                           password=password)
            self.__load_schema()
        except OperationalError as e:
            err_str = "tank is not available"
            logging.error(f"Tank __init__ {err_str}")
            raise TankNotAvailableError(err_str)
        if self.__schema_version < self.__MINIMUM_ALLOWED_SCHEMA_VERSION:
            err_str = f"invalid schema version {self.__schema_version} needs >={self.__MINIMUM_ALLOWED_SCHEMA_VERSION}"
            logging.error(f"Tank __init__ {err_str}")
            raise TankInvalidSchemaError(err_str)

    def __del__(self):
        if self.__conn is not None:
            logging.debug("Tank __del__ closing database connection")
            self.__conn.close()

    def __connect(self,
                  database: str = cfg.tank_database_name,
                  user: str = cfg.tank_database_user,
                  host: str = cfg.tank_database_hostname,
                  password: str = cfg.tank_database_password):
        logging.debug(f"Tank __connect connecting to database {database} on {host}")
        self.__conn = connect(database=database,
                              user=user,
                              host=host,
                              password=password)

    def __does_table_exist(self, table: str):
        with self.__conn.cursor() as cur:
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (table,))
            if cur.fetchone()[0]:
                logging.debug(f"Tank __does_table_exist {table} exists")
                return True
        logging.debug(f"Tank __does_table_exist {table} does not exist")
        return False

    def __load_schema(self):
        self.__sync_schema_version_from_db()

        if self.__schema_version == 0:
            self.__schema_v0_to_v1()
            self.__sync_schema_version_from_db()

        logging.debug(f"Tank __load_schema version {self.__schema_version}")

    def __sync_schema_version_from_db(self) -> int:
        if not self.__does_table_exist("schema_config"):
            logging.debug("Tank __get_schema_version schema_config table does not exist")
            self.__schema_version = 0
        with self.__conn.cursor() as cur:
            cur.execute("""
                        select version from schema_config
                        order by config_id desc 
                        limit 1
            """)
            self.__schema_version = cur.fetchone()[0]

    def __schema_v0_to_v1(self):
        logging.debug("Tank __schema_v0_to_v1 creating new v1 schema")

        with self.__conn.cursor() as cur:
            cur.execute("""
                        create table schema_config(
                            ds date not null default current_date, 
                            config_id serial primary key,
                            version int not null,
                            timestamp timestamp not null default now()
                        )
            """)
            cur.execute("insert into schema_config(version) values (%s)", (1,))

            cur.execute("""
                        create table task_log(
                            ds date not null default current_date, 
                            log_id serial primary key,
                            task_name varchar (100) not null,
                            result integer not null,
                            data jsonb default null,
                            timestamp timestamp not null default now()
                        )
            """)

            self.__conn.commit()

    def log_result(self, task_name: str, result: int, data=None):
        logging.debug(f"Tank log_result logging {task_name} with result {result}")

        data_str = ""
        if data is not None:
            data_str = dumps(data)

        with self.__conn.cursor() as cur:
            cur.execute("""
                        insert into task_log (
                            task_name,
                            result,
                            data
                        )
                        values (
                            %s,
                            %s,
                            %s
                        )
            """, (task_name, result, data_str,))
            self.__conn.commit()

    def has_task_run_ok_today(self, name):
        with self.__conn.cursor() as cur:
            cur.execute("""
                        select log_id
                        from task_log
                        where
                            ds = current_date
                            AND result = %s
                            AND task_name = %s
                        limit 1
            """, (TR_OK, name,))
            if cur.fetchone() is not None:
                logging.debug(f"Tank has_task_run_ok_today {name} has run ok")
                return True
        logging.debug(f"Tank has_task_run_ok_today {name} has not run ok")
        return False
