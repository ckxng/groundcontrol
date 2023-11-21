import logging
from task import global_task_list as task_list
from control.global_status import global_status as st
from control.tank import Tank, TankInvalidSchemaError, TankNotAvailableError
from time import time


def execute_all_one_time():
    try:
        tank = Tank()

        logging.debug("scheduler execute_all_one_time starting")
        for t in task_list:
            data = {
                "name": f"{t.__module__}.{t.__name__}",
                "start_time": time(),
            }
            logging.debug(f"scheduler execute_all_one_time passing control to {data['name']}")
            st.set_current_task(data["name"])
            data["return"], data["response"] = t()
            data["end_time"] = time()
            data["duration"] = data["end_time"] - data["start_time"]
            logging.info(f"scheduler execute_all_one_time {data['name']} ret={data['return']}")
            tank.log_result(task_name=f"{data['name']}", result=data['return'], data=data)

    except TankNotAvailableError as e:
        logging.error(f"scheduler execute_all_one_time error: {e}")
    except TankInvalidSchemaError as e:
        logging.error(f"scheduler execute_all_one_time error: {e}")

    logging.debug("scheduler execute_all_one_time complete")
    st.set_current_task(None)
