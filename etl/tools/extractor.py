import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Iterable

import psycopg2
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extras import DictCursor

from etl.tools.backoff import BOFF_CONFIG, backoff
from etl.tools.config import PostgresConfig
from etl.tools.maker_guery import get_query
from etl.tools.state import State

log = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self, dsl: PostgresConfig, batch_size: int, state: State):
        self.batch_size = batch_size
        self.connection = None
        self.dsl = dsl.dict()
        self.state = state
        self.start_time = None
        self.last_modified = None

    @backoff(**BOFF_CONFIG.dict())
    def connect(self):
        """
        Создается подлючение к Postgres.
        Ошибка если Postgres недоступен.
        :return:
        """
        self.connection = psycopg2.connect(
            **self.dsl,
            cursor_factory=DictCursor
        )

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
                    yield from func(self, *args, **kwargs)
                    break
                except (OperationalError, InterfaceError) as r:
                    log.info(f"Postgres connect ERROR {r}")
                    self.connect()

        return inner

    @_reconnect
    def extractor_films(self) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения фильмов.
        Выборка ограничена датой старта, датой последней проверки,
        лимитом, последним uuid
        :return: возвращает данные фильма в виде словаря
        """
        while True:
            with self.connection.cursor() as curs:
                last_uuid = self.state.get_state("film_work_last_uuid")
                data = [self.last_modified, self.start_time]
                query = get_query("film_work", last_uuid=last_uuid)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(query, data)
                if not curs.rowcount:
                    break
                for _ in range(self.batch_size):
                    row = curs.fetchone()
                    if row is not None:
                        last_row = row
                    yield row
                self.state.set_state("film_work_last_uuid", last_row["id"])

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
            for _ in range(self.batch_size):
                row = curs.fetchone()
                yield row

    @_reconnect
    def _extractor_ids(self, table, where_in=None) -> Iterable[list[str]]:
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
                ids_ = []
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
                    ids_.append(row["id"])
                yield ids_
                self.state.set_state(f"{table}_last_uuid", row["id"])

    def _reference_extractor(
            self,
            reference_tab: str
    ) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения изменений фильмов через 3 запроса
        Сначала происходит выборка изменений в reference таблице,
        Далее собирается список свзаных фильмов через таблицу М2М
        Собираются все фильмы входящие в выборку из прошлого запроса
        :param reference_tab: таблица genre или person
        :return: возвращает данные фильма в виде словаря
        """
        for reference_ids in self._extractor_ids(reference_tab):
            for film_work_ids in self._extractor_ids(
                f"{reference_tab}_film_work", reference_ids
            ):
                yield from self._extractor_films_in(film_work_ids)
            self.state.set_state(f"{reference_tab}_film_work_last_uuid", None)
            log.info("Del ref_film_work_last_uuid")

    def extractors(self) -> Iterable[dict[str, Any]]:
        """
        Функция композитного генератора для получения изменения фильмов.
        В начале итерации создается ограничение в виде временного отрезка.
        Проверка происходит:

            1. От даты старой проверки (если такая имеется)
            2. До начала старта цикла.

        При успешном прохождении всех проверок,
        последняя дата проверки назначается датой старта.
        :return:озвращает данные фильма в виде словаря
        """
        self.last_modified = self.state.get_state("last_modified")
        if self.last_modified is None:
            self.last_modified = datetime(1, 1, 1, tzinfo=timezone.utc)
        self.start_time = self.state.get_state("start_time")
        if self.start_time is None:
            self.start_time = datetime.now(timezone.utc)
            self.state.set_state("start_time", str(self.start_time))
        log.info("Start check films")
        yield from self.extractor_films()
        log.info("End check films")
        # if self.state.get_state("last_modified") is None:
        #     log.info("Last_modified is None,set last_modified")
        #     batch = {
        #         "start_time": None,
        #         "last_modified": str(self.start_time),
        #         "film_work_last_uuid": None,
        #     }
        #     self.state.butch_set_state(batch)
        #     return
        log.info("Start check genre")
        yield from self._reference_extractor("genre")
        log.info("End check genre")
        log.info("Start check person")
        yield from self._reference_extractor("person")
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
        :return:
        """
        self.connection.close()
        log.info("Postgres connection close")

    def __enter__(self):
        """
        Инициирует подклчение к Postgres
        :return:
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрывает подключение к Postgres
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.connection.close()
        log.info("Postgres connection close")
