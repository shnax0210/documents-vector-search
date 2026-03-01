import logging
from typing import Generator

def read_items_in_batches(read_batch_func, 
                          fetch_items_from_result_func, 
                          fetch_total_from_result_func, 
                          batch_size, 
                          max_skipped_items_in_row=3,
                          itemsName="items",
                          cursor_parser=None) -> Generator:
    are_there_more_items_to_read = True
    start_at = 0
    number_of_items_to_read_one_by_one = 0
    skipped_items_in_row = 0
    total = None
    cursor = None

    while are_there_more_items_to_read:
        current_batch_size = batch_size if number_of_items_to_read_one_by_one == 0 else 1
        try:
            if cursor_parser is not None:
                read_result = read_batch_func(start_at, current_batch_size, cursor=cursor)
            else:
                read_result = read_batch_func(start_at, current_batch_size)
        except Exception as e:
            if current_batch_size == 1:
                if skipped_items_in_row >= max_skipped_items_in_row:
                    logging.error(f"Max number of skipped {itemsName} in row ({max_skipped_items_in_row}) was reached. Stopping reading.")
                    raise e

                logging.warning(f"Skipping one of {itemsName} at position {start_at} because of an error: {e}")
                skipped_items_in_row += 1
                start_at += 1
                are_there_more_items_to_read = start_at < total if total is not None else True
            number_of_items_to_read_one_by_one = current_batch_size
            continue

        skipped_items_in_row = 0
        if number_of_items_to_read_one_by_one != 0:
            number_of_items_to_read_one_by_one -= 1

        items = fetch_items_from_result_func(read_result)
        total = fetch_total_from_result_func(read_result)
        cursor = cursor_parser(read_result) if cursor_parser is not None else None

        logging.debug(f"New batch with {len(items)} {itemsName} was read, already read {start_at + len(items)} from {total}")

        for item in items:
            yield item

        start_at = start_at + len(items)
        are_there_more_items_to_read = start_at < total