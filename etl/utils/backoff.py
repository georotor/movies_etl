import logging
from functools import wraps
from time import sleep


def backoff(exceptions=(Exception,), start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Декоратор для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени
    ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exceptions: кортеж отслеживаемых Exception
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            delay = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logging.error(f"backoff, следующая попытка через {delay} сек.", exc_info=True)
                    sleep(delay)

                    delay = min(delay * factor, border_sleep_time)

        return inner

    return func_wrapper
