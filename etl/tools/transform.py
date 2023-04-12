from typing import Any

from psycopg2.extras import DictRow

from etl.tools.models import FilmWorkES, Person, PersonType


class Transform:
    def __init__(self, movie: DictRow):
        self.movie = movie

    def transform(self) -> dict[str, Any]:
        """
        Функция трансформации формата данных
        для загрузки в Elasticsearch.
        :return: данные фильма в виде словаря
        """
        movie = self._pre_validate(self.movie).dict()
        movie["_id"] = movie["id"]
        return movie

    @staticmethod
    def _pre_validate(movie: DictRow) -> FilmWorkES:
        """
        Функция валидирует данные фильма в виде DictRow
        и возвращает данные фильма в моделе FilmWorkES.
        для загрузки в Elasticsearch.
        :param movie: данные фильма в виде словаря
        :return: данные фильма в моделе FilmWorkES
        """
        actors, actors_name = [], []
        writers, writers_name = [], []
        director = []
        for person in movie["persons"]:
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
            id=movie["id"],
            imdb_rating=movie["rating"],
            genre=movie["genres"],
            title=movie["title"],
            description=movie["description"],
            director=director,
            actors_names=actors_name,
            writers_names=writers_name,
            actors=actors,
            writers=writers,
        )
