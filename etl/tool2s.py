import os
import dotenv
import sqlite3
from contextlib import contextmanager
from functools import wraps
from time import sleep

import psycopg2
from psycopg2.extras import DictCursor


@contextmanager
def conn_context_sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            retry = 1
            t = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as r:
                    print(retry)
                    if t < border_sleep_time:
                        t = start_sleep_time * (factor ^ retry)
                    else:
                        t = border_sleep_time
                    sleep(t)
                    retry += 1

        return inner

    return func_wrapper


@backoff
def conn_context_pg(dsl: dict):
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    # return conn
    yield conn
    conn.close()


class Posgress:
    def __init__(self):
        self.connection = None
        self.dsl = load_env_db()
        self._connect()

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def _connect(self):
        self.connection = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)

    @contextmanager
    def get_cursor(self):
        yield self.connection.cursor()

    def execute_iter(self, query, batch_size):
        while True:
            print('eee')
            try:
                with self.get_cursor() as curs:
                    sleep(10)
                    curs.execute(query)
                    for row in curs.fetchmany(batch_size):
                        yield row
                break
            except Exception:
                self._connect()

    def __del__(self):
        try:
            self.connection.close()
        except Exception:
            pass


def load_env_db() -> dict:
    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
    }
    return dsl


dotenv.load_dotenv()
dsl = load_env_db()
conn = Posgress()
# connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
# curs = conn.executes("SELECT * FROM content.person LIMIT 100;")
while True:
    for i in conn.execute_iter("SELECT * FROM content.person LIMIT 100;", 100):
        print(i)
    sleep(2)
# with connection.cursor() as curs:
#     print(type(curs))
#     curs.execute("SELECT * FROM content.person LIMIT 100;")
#     while True:
#         print('now')
#         sql_data = curs.fetchone()
#         for i in sql_data:
#             print(i)
#             sleep(1)

#
# for i in conn.cursor.fetchmany(2):
#     print(i)
