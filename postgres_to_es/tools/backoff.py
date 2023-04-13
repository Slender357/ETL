import logging
from functools import wraps
from time import sleep
from typing import Callable

from postgres_to_es.tools.config import BackOffConfig

BOFF_CONFIG = BackOffConfig()
log = logging.getLogger(__name__)


def backoff(
        start_sleep_time: float,
        factor: int,
        border_sleep_time: int
) -> Callable:
    """
    Функция для повторного выполнения функции
    через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func: Callable) -> Callable:
        @wraps(func)
        def inner(*args, **kwargs):
            retry = 1
            t = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    if t < border_sleep_time:
                        t = start_sleep_time * (factor**retry)
                    else:
                        t = border_sleep_time
                    sleep(t)
                    log.info(f"{error} retry {retry} sleep {t}")
                    retry += 1

        return inner

    return func_wrapper
