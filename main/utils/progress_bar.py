import time

from tqdm import tqdm

def wrap_generator_with_progress_bar(generator, approx_total, progress_bar_name="Processing"):
    progress_bar = tqdm(total=approx_total, desc=progress_bar_name)

    for item in generator:
        yield item
        progress_bar.update(1)

    progress_bar.close()


def wrap_iterator_with_progress_bar(iterator, progress_bar_name="Processing"):
    return tqdm(iterator, desc=progress_bar_name)

