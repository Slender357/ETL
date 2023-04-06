import os
from contextlib import contextmanager
from time import sleep

import dotenv
import psycopg2
from psycopg2.extras import DictCursor

from etl.tools.backoff import backoff

dotenv.load_dotenv()


def load_env_db() -> dict:
    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
    }
    return dsl


class PostgressSotage:
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
