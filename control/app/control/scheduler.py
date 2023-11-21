import logging
from task import global_task_list as task_list
from control.global_status import global_status as st
from control.tank import Tank, TankInvalidSchemaError, TankNotAvailableError
from control.task_return import TR_SKIPPED
from time import time


def execute_all_one_time():

    logging.debug("scheduler execute_all_one_time starting")
    for t in task_list:
        try:
            tank = Tank()
            data = {
                "name": f"{t.__module__}.{t.__name__}",
                "start_time": time(),
            }
            logging.debug(f"scheduler execute_all_one_time passing control to {data['name']}")
            st.set_current_task(data["name"])
            if tank.has_task_run_ok_today(data["name"]):
                data["return"], data["response"] = (TR_SKIPPED, {})
            else:
                data["return"], data["response"] = t()
            data["end_time"] = time()
            data["duration"] = data["end_time"] - data["start_time"]
            logging.info(f"scheduler execute_all_one_time {data['name']} ret={data['return']}")
            tank.log_result(task_name=f"{data['name']}", result=data['return'], data=data)

        except TankNotAvailableError as e:
            logging.error(f"scheduler execute_all_one_time {t.__module__}.{t.__name__} tank not available: {e}")
        except TankInvalidSchemaError as e:
            logging.error(f"scheduler execute_all_one_time {t.__module__}.{t.__name__} invalid schema: {e}")
        except Exception as e:
            logging.critical(f"scheduler execute_all_one_time {t.__module__}.{t.__name__} unknown error: {e}")

    logging.debug("scheduler execute_all_one_time complete")
    st.set_current_task(None)
