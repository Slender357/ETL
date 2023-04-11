from functools import wraps
from typing import Any, Callable, Optional

from elasticsearch import Elasticsearch, helpers

from etl.tools.backoff import BOFF_CONFIG, backoff
from etl.tools.config import ESConfig


class Loader:
    def __init__(self, config: ESConfig):
        self.config = config
        self.connection: Optional[Elasticsearch] = None

    @backoff(**BOFF_CONFIG.dict())
    def connect(self):
        self.connection = Elasticsearch(
            f"{self.config.host}:{self.config.port}"
        )
        if not self.connection.ping():
            raise

    @staticmethod
    def _reconnect(func: Callable) -> Callable:
        @wraps(func)
        def inner(self, *args, **kwargs):
            while True:
                try:
                    return func(self, *args, **kwargs)
                except Exception as r:
                    print(r)
                    self.connect()

        return inner

    def loader(self, data: list[dict[str, Any]]) -> None:
        while True:
            self.bulk(data)

    @_reconnect
    def bulk(self, data: list[dict[str, Any]]) -> None:
        resp = helpers.bulk(self.connection, index='movies', actions=data)
        print(resp)

    def __enter__(self):
        self.connect()
        return self

    def __del__(self):
        self.connection.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
