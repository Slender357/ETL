import json
import logging
from functools import wraps
from time import sleep
from typing import Any, Callable, Optional

from elasticsearch import BadRequestError, Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

from postgres_to_es.tools.backoff import BOFF_CONFIG, backoff
from postgres_to_es.tools.config import BASE_DIR, ESConfig

log = logging.getLogger(__name__)


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
                except Exception as error:
                    log.info(f"Elasticsearch connect ERROR {error}")
                    self._connect()

        return inner

    @_reconnect
    def bulk(self, data: list[dict[str, Any]]) -> None:
        """
        Отправка данных в Elasticsearch.
        Если возникнут ошибки при загрузке данных,
        повториться попытка загрузки через
        self.config.bulk_retrys_sleep секунд.
        При максимальном количестве попыток произойдет разрыв подклчения.
        :param data: список объектов для загрузки
        """
        retry = 0
        while True:
            ok, errors = helpers.bulk(
                self.connection,
                index="movies",
                actions=data,
                raise_on_error=False
            )
            if len(errors) != 0:
                log.info(
                    f"Elasticsearch dont save "
                    f"{len(errors)} document, try again"
                )
                if retry < self.config.bulk_max_retrys:
                    retry += 1
                    sleep(self.config.bulk_retrys_sleep)
                    continue
                else:
                    log.info(errors)
                    self.connection.close()
                    log.info("Elasticsearch connection close")
                    raise BulkIndexError
            log.info(f"Elasticsearch save {ok} document")
            break

    @_reconnect
    def create_movie_index(self):
        try:
            with open(BASE_DIR.joinpath("es_shema.json")) as j:
                es_shema = json.load(j)
                self.connection.indices.create(
                    settings=es_shema["settings"],
                    mappings=es_shema["mappings"],
                    index="movies",
                )
        except BadRequestError:
            pass

    def __enter__(self):
        """
        Иницирует подклчение Elasticsearch.
        Пытается создать индекс.
        :return: self
        """
        self._connect()
        self.create_movie_index()
        return self

    def __del__(self):
        """
        Закрывает подключение к Elasticsearch.
        """
        self.connection.close()
        log.info("Elasticsearch connection close")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрывает подключение к Elasticsearch.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        """
        self.connection.close()
        log.info("Elasticsearch connection close")
