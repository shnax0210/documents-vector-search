import time
import logging
from typing import Any

def execute_and_measure_duration(func) -> tuple:
    start_time = time.time()

    result = None
    error = None

    try:
        result = func()
    except Exception as ex:
        error = ex

    end_time = time.time()

    return result, error, end_time - start_time


def log_execution_duration(func, identifier, enabled=True) -> Any:
    if enabled:
        logging.info('------------------------------------------------------------------')
        logging.info(f'Started "{identifier}"')

    result, error, duration = execute_and_measure_duration(func)

    if enabled:
        logging.info(f'Finished "{identifier}" in {duration} seconds with {'success' if error is None else 'error'} result')
        logging.info('------------------------------------------------------------------')

    if error is not None:
        raise error

    return result