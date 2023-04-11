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
    def _connect(self):
        """
        Создается подлючение к Elasticsearch.
        Ошибка если ES недоступен.
        :return:
        """
        self.connection = Elasticsearch(
            f"{self.config.host}:{self.config.port}"
        )
        if not self.connection.ping():
            raise

    @staticmethod
    def _reconnect(func: Callable) -> Callable:
        """
        Попытка выполнить функцию.
        При неудаче происходит подключение к базе до тех пор,
        пока база не будет доступна.
        :param func:
        :return:
        """
        @wraps(func)
        def inner(self, *args, **kwargs):
            while True:
                try:
                    return func(self, *args, **kwargs)
                except Exception as r:
                    print(r)
                    self.connect()

        return inner

    # def loader(self, data: list[dict[str, Any]]) -> None:
    #     """
    #     Функция бесконеного цикла отправки данных
    #     :param data:
    #     :return:
    #     """
    #     while True:
    #         self._bulk(data)

    @_reconnect
    def bulk(self, data: list[dict[str, Any]]) -> None:
        """
        Отправка данных в Elasticsearch.
        :param data:
        :return:
        """
        resp = helpers.bulk(self.connection, index='movies', actions=data)
        print(resp)

    def __enter__(self):
        """
        Иницирует подклчение Elasticsearch.
        :return:
        """
        self._connect()
        return self

    def __del__(self):
        self.connection.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрывает подключение к Elasticsearch.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.connection.close()
