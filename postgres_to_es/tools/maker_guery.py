def get_query(table: str, last_uuid: str = None, where_in: list = None) -> str:
    """
    Функция создания query в зависимоти от параметров.
    Все запросы сортируются по uuid
    :param table: название таблицы сбора данных
    :param last_uuid: последний uuid из прошлой выборки для ограничения
    :param where_in: список id данные которых необходимо получить
    :return:
    """
    query = ""
    if table == "genre" or table == "person":
        query = f"""
        SELECT id, modified
        FROM content.{table}
        WHERE modified > %s AND modified <= %s"""
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
            COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'genre_name', g.name,
                       'genre_id', g.id
                   )
               ) FILTER (WHERE g.id is not null),
               '[]'
            ) as genres
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
            query += " fw.modified > %s AND fw.modified <= %s"
            if last_uuid is not None:
                query += " AND fw.id > %s"
            query += " GROUP BY fw.id ORDER BY fw.id LIMIT %s"
    return query


def get_query_single(table: str, last_uuid: str = None) -> str:
    """
    Функция создания query запроса в таблицу без зависимостей.
    Все запросы сортируются по uuid
    :param table: название таблицы сбора данных
    :param last_uuid: последний uuid из прошлой выборки для ограничения
    :param where_in: список id данные которых необходимо получить
    :return:
    """
    query = ""
    if table == "person":
        query = """
                SELECT
                    sng.id,
                    sng.full_name,
                    sng.modified
                FROM content.person sng
                WHERE sng.modified > %s AND sng.modified <= %s"""
        if last_uuid is not None:
            query += " AND sng.id > %s"
    elif table == "genre":
        query = """
                SELECT
                    sng.id,
                    sng.name,
                    sng.description,
                    sng.modified
                FROM content.genre sng
                WHERE sng.modified > %s AND sng.modified <= %s"""
        if last_uuid is not None:
            query += " AND sng.id > %s"
    query += " ORDER BY sng.id LIMIT %s"
    return query
