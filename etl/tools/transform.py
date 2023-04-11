from typing import Any, Iterable

from etl.tools.models import FilmWorkES, Person, PersonType


class Transform:
    def __init__(self, iterator: Iterable[dict[str, Any]]):
        self.iterator = iterator

    def transform(self) -> Iterable[dict[str, Any]]:
        """
        Функция генератор трансформации формата данных
        для загрузки в Elasticsearch.
        :return: данные фильма в виде словаря
        """
        for row in self.iterator:
            movie = self._pre_validate(row)
            if movie is not None:
                movie["_id"] = movie["id"]
            yield movie

    @staticmethod
    def _pre_validate(row: dict[str, Any] | None) -> dict[str, Any] | None:
        """
        Функция валидирует данные фильма в виде словаря
        и возвращает данные фильма в моделе FilmWorkES.
        для загрузки в Elasticsearch.
        :param row: данные фильма в виде словаря
        :return: данные фильма в моделе FilmWorkES
        """
        if row is None:
            return row
        actors, actors_name = [], []
        writers, writers_name = [], []
        director = []
        for person in row["persons"]:
            match person["person_role"]:
                case PersonType.actor.value:
                    actors.append(
                        Person(
                            id=person["person_id"],
                            name=person["person_name"]
                        )
                    )
                    actors_name.append(person["person_name"])
                case PersonType.writer.value:
                    writers.append(
                        Person(
                            id=person["person_id"],
                            name=person["person_name"]
                        )
                    )
                    writers_name.append(person["person_name"])
                case PersonType.director.value:
                    director.append(person["person_name"])
        return FilmWorkES(
            id=row["id"],
            imdb_rating=row["rating"],
            genre=row["genres"],
            title=row["title"],
            description=row["description"],
            director=director,
            actors_names=actors_name,
            writers_names=writers_name,
            actors=actors,
            writers=writers,
        ).dict()
