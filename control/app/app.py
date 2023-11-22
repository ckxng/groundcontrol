#!/usr/bin/env python3
from control.global_config import global_config as cfg
import logging

# log to file and stderr
logging.basicConfig(
    level=cfg.logging_level,
    datefmt='%Y-%m-%dT%H:%M:%S',
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(cfg.logging_file),
        logging.StreamHandler()
    ]
)

from control.status import S_RUNNING, S_STARTING, S_STOPPING, S_SLEEPING
from control.global_status import global_status as st
from control.scheduler import execute_all_one_time
from sys import exit
from signal import signal, SIGINT
from time import sleep


def signal_handler(sig, frame):
    st.set_state(S_STOPPING)
    exit(0)


signal(SIGINT, signal_handler)

st.set_state(S_STARTING)

while True:
    st.set_state(S_RUNNING)
    execute_all_one_time()
    st.set_state(S_SLEEPING)
    sleep(cfg.run_interval)
