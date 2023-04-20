import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Iterable, Optional

import psycopg2
from more_itertools import chunked
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from postgres_to_es.tools.backoff import backoff, boff_config
from postgres_to_es.tools.config import PostgresConfig
from postgres_to_es.tools.maker_guery import get_query, get_query_single
from postgres_to_es.tools.state import State
from postgres_to_es.tools.transform import Transform

log = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self, dsl: PostgresConfig, batch_size: int, state: State):
        self.batch_size = batch_size
        self.connection: Optional[_connection] = None
        self.dsl = dsl.dict()
        self.state = state
        self.start_time: Optional[datetime] = None
        self.last_modified: Optional[datetime] = None

    @backoff(**boff_config.dict())
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
                yield kwargs["index"], items
                try:
                    last_uuid = items[-1]["id"]
                except TypeError:
                    last_uuid = items[-1]
                self.state.set_state(
                    f"{kwargs['table']}_{kwargs['index']}_last_uuid",
                    str(last_uuid)
                )

        return inner

    @_reconnect
    @chunk_decor
    def extractor_films(
        self, table: str, index: str, last_uuid=None
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
                    last_uuid = self.state.get_state(
                        f"{table}_{index}_last_uuid"
                    )
                data = [self.last_modified, self.start_time]
                query = get_query(table, last_uuid=last_uuid)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(query, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    yield Transform(row, index).transform()
                last_uuid = row["id"]

    @_reconnect
    @chunk_decor
    def extractor_single_table(
        self, table: str, index: str, last_uuid=None
    ) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения данных таблиц без зависимых связей.
        Выборка ограничена датой старта, датой последней проверки,
        лимитом, последним uuid
        :return: возвращает данные в виде словаря
        """
        while True:
            with self.connection.cursor() as curs:
                if last_uuid is None:
                    last_uuid = self.state.get_state(
                        f"{table}_{index}_last_uuid"
                    )
                data = [self.last_modified, self.start_time]
                query = get_query_single(table=table, last_uuid=last_uuid)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(query, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    yield Transform(row, index).transform()
                last_uuid = row["id"]

    @_reconnect
    def _extractor_films_in(
        self, index: str, in_films: list[str]
    ) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения фильмов.
        Выборка ограничена списком uuid фильмов
        :param in_films: список запрашиваемых фильмов
        :return: возвращает данные фильма в виде словаря
        """
        with self.connection.cursor() as curs:
            query = get_query("film_work", where_in=in_films)
            curs.execute(query, in_films)
            yield index, (
                Transform(row, "movies").transform() for row in curs.fetchall()
            )

    @_reconnect
    @chunk_decor
    def _extractor_ids(
        self, table: str, index: str, last_uuid: str = None, where_in=None
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
                    last_uuid = self.state.get_state(
                        f"{table}_{index}_last_uuid"
                    )
                if where_in is None:
                    data = [self.last_modified, self.start_time]
                else:
                    data = [i for i in where_in]
                query = get_query(
                    table=table,
                    last_uuid=last_uuid,
                    where_in=data
                )
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
        self,
        reference_tab: str,
        index: str,
    ) -> Iterable[dict[str, Any]]:
        """
        Функция генератор для получения изменений фильмов через 3 запроса
        Сначала происходит выборка изменений в reference таблице,
        Далее собирается список свзаных фильмов через таблицу М2М
        Собираются все фильмы входящие в выборку из прошлого запроса
        :param reference_tab: таблица genre или person
        :return: возвращает список объектов для записи
        """
        for _, reference_ids in self._extractor_ids(
                table=reference_tab,
                index=index
        ):
            for _, film_work_ids in self._extractor_ids(
                table=f"{reference_tab}_film_work",
                    index=index,
                    where_in=reference_ids
            ):
                yield from self._extractor_films_in(
                    index=index,
                    in_films=film_work_ids
                )
            self.state.set_state(
                f"{reference_tab}_film_work_{index}_last_uuid",
                None
            )
            log.info(f"Del reference UUID from {reference_tab}_film_work")

    def extractors(self) -> Iterable[tuple[str, list[dict[str, Any]]]]:
        """
        Функция композитного генератора для получения изменения фильмов.
        В начале итерации создается ограничение в виде временного отрезка.
        Проверка происходит:

            1. От даты старой проверки (если такая имеется)
            2. До начала старта цикла.

        При успешном прохождении всех проверок,
        последняя дата проверки назначается датой старта.
        :return:возвращает индекс и список объектов для записи
        """
        self.last_modified = self.state.get_state("last_modified")
        if self.last_modified is None:
            self.last_modified = datetime(1, 1, 1, tzinfo=timezone.utc)
        self.start_time = self.state.get_state("start_time")
        if self.start_time is None:
            self.start_time = datetime.now(timezone.utc)
            self.state.set_state("start_time", str(self.start_time))
        batch_state = {
            "start_time": None,
            "last_modified": str(self.start_time),
            "film_work_movies_last_uuid": None,
            "genre_movies_last_uuid": None,
            "genre_film_work_movies_last_uuid": None,
            "person_movies_last_uuid": None,
            "person_film_work_movies_last_uuid": None,
            "person_persons_last_uuid": None,
            "genre_genres_last_uuid": None,
        }
        log.info("Start check films")
        yield from self.extractor_films(table="film_work", index="movies")
        log.info("End check films")
        log.info("Start check persons")
        yield from self.extractor_single_table(table="person", index="persons")
        log.info("End check persons")
        log.info("Start check genres")
        yield from self.extractor_single_table(table="genre", index="genres")
        log.info("End check genres")
        if self.state.get_state("last_modified") is None:
            log.info("Last_modified is None,set last_modified")
            self.state.butch_set_state(batch_state)
            return
        log.info("Start check modified genre for film")
        yield from self._reference_extractor(
            reference_tab="genre",
            index="movies"
        )
        log.info("End check modified genre for film")
        log.info("Start check modified person for film")
        yield from self._reference_extractor(
            reference_tab="person",
            index="movies"
        )
        log.info("End check modified person for film")
        log.info("Set Last_modified")
        self.state.butch_set_state(batch_state)

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
