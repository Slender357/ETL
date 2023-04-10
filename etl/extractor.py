import datetime
from time import sleep
from more_itertools import chunked
from itertools import chain
from typing import Any, Optional

from etl.tools.postgr import PostgresExtractor
from etl.tools.state import State, JsonFileStorage


def get_quary(data: list[Any], last_id: Optional[str], batch_size: int) -> str:
    quary = """SELECT
                               fw.id,
                               fw.title,
                               fw.description,
                               fw.rating,
                               fw.type,
                               fw.created,
                               fw.modified,
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
                            WHERE fw.modified > %s"""
    if last_id is not None:
        quary += " AND fw.id > %s"
        data.append(last_id)
    quary += " GROUP BY fw.id ORDER BY fw.id"
    quary += f" LIMIT {batch_size}"
    return quary


# def extractor():
#     postgr = PostgrConnection()
#     state = State(JsonFileStorage('./state.json'))
#     last_date = state.get_state('last_date')
#     last_id = state.get_state('last_id')
#     if last_date is None:
#         last_date = datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
#     max_date = last_date
#     while True:
#         try:
#             with postgr.connection.cursor() as curs:
#                 while True:
#                     data = [last_date]
#                     quary = f"SELECT id, modified FROM content.genre WHERE modified >= %s"
#                     if last_id is not None:
#                         quary += " AND id > %s"
#                         data.append(last_id)
#                     quary += " ORDER BY id LIMIT 5"
#                     curs.execute(quary, data)
#                     if not curs.rowcount:
#                         break
#                     for row in curs.fetchall():
#                         max_date = max(row['modified'], max_date)
#                         yield row
#                     state.set_state('last_id', row['id'])
#                     last_id = row['id']
#                 state.set_state('last_date', str(max_date))
#                 break
#         except OperationalError:
#             postgr.reconnect()
def extractor_ids(table: str, batch_size: int, postgr):
    state = State(JsonFileStorage('./state.json'))
    last_date = state.get_state(f'{table}_last_date')
    last_id = state.get_state(f'{table}_last_id')
    if last_date is None:
        last_date = datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
    max_date = last_date
    try:
        with postgr.connection.cursor() as curs:
            while True:
                data = [table, last_date]
                quary = f"SELECT id, modified FROM content.%s WHERE modified >= %s"
                if last_id is not None:
                    quary += " AND id > %s"
                    data.append(last_id)
                quary += f" ORDER BY id LIMIT {batch_size}"
                curs.execute(quary, data)
                if not curs.rowcount:
                    break
                for row in curs.fetchall():
                    max_date = max(row['modified'], max_date)
                    yield row['id']
                state.set_state(f'{table}_last_id', row['id'])
                last_id = row['id']
            state.set_state(f'{table}_last_date', str(max_date))
    except:
        postgr.reconnect()


# def extractor_2(table: str, batch_size: int, postgr):
#     state = State(JsonFileStorage('./state.json'))
#     last_date = state.get_state(f'{table}_last_date')
#     last_id = state.get_state(f'{table}_last_id')
#     if last_date is None:
#         last_date = datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
#     max_date = last_date
#     with postgr.connection.cursor() as curs:
#         while True:
#             data = [last_date]
#             quary = f"SELECT id, modified FROM content.{table} WHERE modified >= %s"
#             if last_id is not None:
#                 quary += " AND id > %s"
#                 data.append(last_id)
#             quary += f" ORDER BY id LIMIT {batch_size}"
#             while True:
#                 try:
#                     curs.execute(quary, data)
#                     break
#                 except:
#                     postgr.reconnect()
#             if not curs.rowcount:
#                 break
#             for row in curs.fetchall():
#                 max_date = max(row['modified'], max_date)
#                 yield row['id']
#             state.set_state(f'{table}_last_id', row['id'])
#             last_id = row['id']
#         state.set_state(f'{table}_last_date', str(max_date))


# for i in extractor_ids('person', 50, postgr):
#     print(i)

# for items in chunked:
#     ids_ = ",".join(['%s' for _ in items])
#     quary = f"""SELECT fw.id, fw.modified
#     FROM content.film_work fw
#     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
#     WHERE pfw.person_id IN ({ids_})
#     ORDER BY fw.modified
#     LIMIT 100; """
#     curs.execute(quary, items)
#     for i in curs.fetchall():
#         print(i)
def extractor_films(table: str, batch_size: int, postgr):
    state = State(JsonFileStorage('./state.json'))
    last_date = state.get_state(f'{table}_last_date')
    last_id = state.get_state(f'{table}_last_id')
    if last_date is None:
        last_date = datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
    max_date = last_date
    while True:
        try:
            with postgr.connection.cursor() as curs:
                while True:
                    data = [last_date]
                    quary = get_quary(data, last_id, batch_size)
                    curs.execute(quary, data)
                    if not curs.rowcount:
                        break
                    for row in curs.fetchall():
                        max_date = max(row['modified'], max_date)
                        yield dict(row)
                    state.set_state(f'{table}_last_id', row['id'])
                    last_id = row['id']
                state.set_state(f'{table}_last_date', str(max_date))
            break
        except Exception as r:
            print(r)
            postgr.connect()


with PostgresExtractor() as con:
    # for i in extractor_films('film_work', 50, con):
    #     print(i)
    #     sleep(1)
    # print('1111')
    for i in con.extractor_films('film_work', 5):
        print(i)
        # print(FilmworkPG(*i))
        sleep(1)
# # for i in extractor_ids('person', 50):
# #     sleep(1)
# #     print(i, k)
# #     k += 1
#
# # state = State(JsonFileStorage('./state.json'))
# # state.set_state('last_date', '1')
# # state.set_state('last_id', '1')
