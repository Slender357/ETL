import os
from datetime import datetime, timezone
from functools import wraps

import dotenv
import psycopg2
from etl.tools.backoff import backoff
from etl.tools.state import JsonFileStorage, State
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extras import DictCursor

dotenv.load_dotenv()


def get_query(table: str, last_uuid: str = None, where_in: list = None) -> str:
    query = ""
    if table == "genre" or table == "person":
        query = f"""
        SELECT id, modified
        FROM content.{table}
        WHERE modified > %s AND modified < %s"""
        if last_uuid is not None:
            query += " AND id > %s"
        query += " ORDER BY id LIMIT %s;"
    if table == "genre_film_work" or table == "person_film_work":
        query = f"""SELECT DISTINCT fw.id as id
                FROM content.film_work fw
                LEFT JOIN content.{table} rfw ON rfw.film_work_id = fw.id
                WHERE"""
        if where_in is not None:
            ref_id = "genre_id" if table == "genre_film_work" else "person_id"
            query += f" rfw.{ref_id} IN "
            query += f"({', '.join('%s' for _ in where_in)})"
        if last_uuid is not None:
            query += " AND fw.id > %s"
        query += " ORDER BY fw.id LIMIT %s;"
    if table == "film_work":
        query = """
        SELECT
        fw.id,
        fw.title,
        fw.description,
        fw.rating,
        fw.type,
        fw.created,
        fw.modified as modified,
        COALESCE (
           json_agg(
               DISTINCT jsonb_build_object(
                   'person_role', pfw.role,
                   'person_id', p.id,
                   'person_name', p.full_name
               )
           ) FILTER (WHERE p.id is not null),
           '[]'
        ) as persons,
        array_agg(DISTINCT g.name) as genres
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE"""
        if where_in is not None:
            query += " fw.id IN "
            query += f"({', '.join('%s' for _ in where_in)})"
            query += " GROUP BY fw.id"
        else:
            query += " fw.modified > %s AND fw.modified < %s"
            if last_uuid is not None:
                query += " AND fw.id > %s"
            query += " GROUP BY fw.id ORDER BY fw.id LIMIT %s"
    return query


def load_env_db() -> dict:
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    return dsl


class PostgresExtractor:
    def __init__(self, batch_size: int, batch_size_genre: int):
        self.batch_size = batch_size
        self.batch_size_genre = batch_size_genre
        self.connection = None
        self.dsl = load_env_db()
        self.state = State(JsonFileStorage("./state.json"))
        self.start_time = None
        self.last_modified = None

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def connect(self):
        self.connection = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)

    @staticmethod
    def _reconnect(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            while True:
                try:
                    yield from func(self, *args, **kwargs)
                    break
                except (OperationalError, InterfaceError) as r:
                    print(r)
                    self.connect()

        return inner

    @_reconnect
    def extractor_films(self):
        with self.connection.cursor() as curs:
            while True:
                last_uuid = self.state.get_state("film_work_last_uuid")
                data = [self.last_modified, self.start_time]
                quary = get_query("film_work", last_uuid=last_uuid)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(quary, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    yield dict(row)
                self.state.set_state("film_work_last_uuid", row["id"])

    @_reconnect
    def _extractor_films_in(self, in_films):
        with self.connection.cursor() as curs:
            quary = get_query("film_work", where_in=in_films)
            curs.execute(quary, in_films)
            for row in curs.fetchall():
                yield dict(row)

    @_reconnect
    def _extractor_ids(self, table, where_in=None):
        with self.connection.cursor() as curs:
            while True:
                ids_ = []
                last_uuid = self.state.get_state(f"{table}_last_uuid")
                if where_in is None:
                    data = [self.last_modified, self.start_time]
                else:
                    data = [i for i in where_in]
                quary = get_query(table, last_uuid=last_uuid, where_in=data)
                if last_uuid is not None:
                    data.append(last_uuid)
                data.append(self.batch_size)
                curs.execute(quary, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    ids_.append(row["id"])
                yield ids_
                self.state.set_state(f"{table}_last_uuid", row["id"])

    def _reference_extractor(self, reference_tab: str):
        for reference_ids in self._extractor_ids(reference_tab):
            for film_work_ids in self._extractor_ids(
                    f"{reference_tab}_film_work", reference_ids
            ):
                yield from self._extractor_films_in(film_work_ids)
            self.state.set_state(f"{reference_tab}_film_work_last_uuid", None)

    def extractors(self):
        self.last_modified = self.state.get_state("last_modified")
        if self.last_modified is None:
            self.last_modified = datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        self.start_time = self.state.get_state("start_time")
        if self.start_time is None:
            self.start_time = datetime.now(timezone.utc)
            self.state.set_state("start_time", str(self.start_time))
        yield from self.extractor_films()
        if self.state.get_state("last_modified") is None:
            batch = {
                "start_time": None,
                "last_modified": str(self.start_time),
                "film_work_last_uuid": None,
            }
            self.state.butch_set_state(batch)
            return
        yield from self._reference_extractor("genre")
        yield from self._reference_extractor("person")
        batch = {
            "start_time": None,
            "last_modified": str(self.start_time),
            "film_work_last_uuid": None,
            "genre_last_uuid": None,
            "person_last_uuid": None,
        }
        self.state.butch_set_state(batch)

    def __del__(self):
        self.connection.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
