import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Iterable, Optional

import psycopg2
from more_itertools import chunked
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from etl.tools.backoff import BOFF_CONFIG, backoff
from etl.tools.config import PostgresConfig
from etl.tools.maker_guery import get_query
from etl.tools.state import State
from etl.tools.transform import Transform

log = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self, dsl: PostgresConfig, batch_size: int, state: State):
        self.batch_size = batch_size
        self.connection: Optional[_connection] = None
        self.dsl = dsl.dict()
        self.state = state
        self.start_time: Optional[datetime] = None
        self.last_modified: Optional[datetime] = None

    @backoff(**BOFF_CONFIG.dict())
    def connect(self):
        """
        Создается подлючение к Postgres.
        Ошибка если Postgres недоступен.
        """
        self.connection = psycopg2.connect(
            **self.dsl,
            cursor_factory=DictCursor
        )

    @staticmethod
    def _reconnect(func: Callable) -> Callable:
        """
        Попытка выполнить функцию генератор.
        При неудаче происходит подключение к базе до тех пор,
        пока база не будет доступна.
        :param func: функция генератор
        :return: результат выполнения функции
        """

        @wraps(func)
        def inner(self, *args, **kwargs):
            while True:
                try:
                    yield from func(self, *args, **kwargs)
                    break
                except (OperationalError, InterfaceError) as r:
                    log.info(f"Postgres connect ERROR {r}")
                    self.connect()

        return inner

    @staticmethod
    def chunk_decor(func: Callable) -> Callable:
        """
        Функция чанкизирует данные генераторов
        и отдает лист объектов для записи
        при успехе устанавливает в стейт последий UUID
        отданной пачки в соответсвующую таблицу
        :param func: функция генератор
        :return: лист объектов для записи
        """
        @wraps(func)
        def inner(self, *args, **kwargs):
            chunk_items = chunked(func(self, *args, **kwargs), self.batch_size)
            for items in chunk_items:
                yield items
                try:
                    last_uuid = items[-1]["id"]
                except TypeError:
                    last_uuid = items[-1]
                self.state.set_state(
                    f"{kwargs['table']}_last_uuid",
                    str(last_uuid)
                )

        return inner

    @_reconnect
    @chunk_decor
    def extractor_films(
            self, table: str,
            last_uuid=None
    ) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения фильмов.
        Выборка ограничена датой старта, датой последней проверки,
        лимитом, последним uuid
        :return: возвращает данные фильма в виде словаря
        """
        while True:
            with self.connection.cursor() as curs:
                if last_uuid is None:
                    last_uuid = self.state.get_state(f"{table}_last_uuid")
                data = [self.last_modified, self.start_time]
                query = get_query(table, last_uuid=last_uuid)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(query, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    yield Transform(row).transform()
                last_uuid = row["id"]
                # self.state.set_state("film_work_last_uuid", row["id"])

    @_reconnect
    def _extractor_films_in(self, in_films) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения фильмов.
        Выборка ограничена списком uuid фильмов
        :param in_films: список запрашиваемых фильмов
        :return: возвращает данные фильма в виде словаря
        """
        with self.connection.cursor() as curs:
            query = get_query("film_work", where_in=in_films)
            curs.execute(query, in_films)
            for row in curs.fetchall():
                yield Transform(row).transform()

    @_reconnect
    @chunk_decor
    def _extractor_ids(
        self, table: str, last_uuid: str = None, where_in=None
    ) -> Iterable[list[str]]:
        """
        Функция генератор для получения списка uuid данной таблицы.
        Выборка выборка может быть ограничена 2 спосбомаи:
        1.  Ограничена датой старта, датой последней проверки,
            лимитом, последним uuid.
        2.  Списком uuid, последним uuid, лимитом
        :param table: название таблицы
        :param where_in: список uuid
        :return: список uuid
        """
        while True:
            with self.connection.cursor() as curs:
                if last_uuid is None:
                    last_uuid = self.state.get_state(f"{table}_last_uuid")
                if where_in is None:
                    data = [self.last_modified, self.start_time]
                else:
                    data = [i for i in where_in]
                query = get_query(table, last_uuid=last_uuid, where_in=data)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(query, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    yield row["id"]
                last_uuid = row["id"]

    def _reference_extractor(
            self, reference_tab: str
    ) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения изменений фильмов через 3 запроса
        Сначала происходит выборка изменений в reference таблице,
        Далее собирается список свзаных фильмов через таблицу М2М
        Собираются все фильмы входящие в выборку из прошлого запроса
        :param reference_tab: таблица genre или person
        :return: возвращает список объектов для записи
        """
        for reference_ids in self._extractor_ids(table=reference_tab):
            for film_work_ids in self._extractor_ids(
                table=f"{reference_tab}_film_work", where_in=reference_ids
            ):
                yield from chunked(
                    self._extractor_films_in(in_films=film_work_ids),
                    self.batch_size
                )
            self.state.set_state(f"{reference_tab}_film_work_last_uuid", None)
            log.info(f"Del reference UUID from {reference_tab}_film_work")

    def extractors(self) -> Iterable[list[dict[str, Any]]]:
        """
        Функция композитного генератора для получения изменения фильмов.
        В начале итерации создается ограничение в виде временного отрезка.
        Проверка происходит:

            1. От даты старой проверки (если такая имеется)
            2. До начала старта цикла.

        При успешном прохождении всех проверок,
        последняя дата проверки назначается датой старта.
        :return:возвращает список объектов для записи
        """
        self.last_modified = self.state.get_state("last_modified")
        if self.last_modified is None:
            self.last_modified = datetime(1, 1, 1, tzinfo=timezone.utc)
        self.start_time = self.state.get_state("start_time")
        if self.start_time is None:
            self.start_time = datetime.now(timezone.utc)
            self.state.set_state("start_time", str(self.start_time))
        log.info("Start check films")
        yield from self.extractor_films(table="film_work")
        log.info("End check films")
        if self.state.get_state("last_modified") is None:
            log.info("Last_modified is None,set last_modified")
            batch = {
                "start_time": None,
                "last_modified": str(self.start_time),
                "film_work_last_uuid": None,
            }
            self.state.butch_set_state(batch)
            return
        log.info("Start check genre")
        yield from self._reference_extractor(reference_tab="genre")
        log.info("End check genre")
        log.info("Start check person")
        yield from self._reference_extractor(reference_tab="person")
        log.info("End check person")
        batch = {
            "start_time": None,
            "last_modified": str(self.start_time),
            "film_work_last_uuid": None,
            "genre_last_uuid": None,
            "person_last_uuid": None,
        }
        log.info("Set Last_modified")
        self.state.butch_set_state(batch)

    def __del__(self):
        """
        Закрывает подклчение к Postgres
        """
        self.connection.close()
        log.info("Postgres connection close")

    def __enter__(self):
        """
        Инициирует подклчение к Postgres
        :return: self
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрывает подключение к Postgres
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        """
        self.connection.close()
        log.info("Postgres connection close")
