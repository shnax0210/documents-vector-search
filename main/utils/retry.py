import time
import logging
from typing import Any

def execute_with_retry(func, func_identifier, retries=3, delay=1) -> Any:
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            logging.warning(f'Attempt of "{func_identifier}" number {attempt + 1} failed: {e}')
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logging.error(f"All {retries} attempts failed.")
                raise e