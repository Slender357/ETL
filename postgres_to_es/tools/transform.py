from typing import Any

from psycopg2.extras import DictRow

from postgres_to_es.tools.models import (
    FilmWorkES,
    GenresES,
    Person,
    PersonES,
    PersonType,
)


class Transform:
    def __init__(self, movie: DictRow, index: str):
        self.raw_data = movie
        self.index = index

    def transform(self) -> dict[str, Any] | None:
        """
        Функция трансформации формата данных
        для загрузки в Elasticsearch.
        :return: данные в виде словаря
        """
        match self.index:
            case "movies":
                return self._add_elastic_id(
                    self._pre_validate_movie(self.raw_data)
                )
            case "persons":
                return self._add_elastic_id(
                    self._pre_validate_person(self.raw_data)
                )
            case "genres":
                return self._add_elastic_id(
                    self._pre_validate_genre(self.raw_data)
                )

    @staticmethod
    def _pre_validate_person(raw_data: DictRow) -> dict[str, Any]:
        """
        Функция валидирует данные через модель PersonES
        и возвращает данные фильма в виде словаря.
        для загрузки в Elasticsearch.
        :return: данные в виде словаря
        """
        return PersonES(
            id=raw_data["id"],
            full_name=raw_data["full_name"]
        ).dict()

    @staticmethod
    def _add_elastic_id(valid_data: dict[str, Any]):
        """
        Функция добавляет _id = id
        :return: данные в виде словаря
        """
        valid_data["_id"] = valid_data["id"]
        return valid_data

    @staticmethod
    def _pre_validate_genre(raw_data: DictRow) -> dict[str, Any]:
        """
        Функция валидирует данные через модель GenresES
        и возвращает данные фильма в виде словаря.
        для загрузки в Elasticsearch.
        :return: данные в виде словаря
        """
        return GenresES(
            id=raw_data["id"],
            name=raw_data["name"],
            description=raw_data["description"],
        ).dict()

    @staticmethod
    def _pre_validate_movie(raw_data: DictRow) -> dict[str, Any]:
        """
        Функция валидирует данные через модель FilmWorkES
        и возвращает данные фильма в виде словаря.
        для загрузки в Elasticsearch.
        :return: данные в виде словаря
        """
        actors, actors_name = [], []
        writers, writers_name = [], []
        directors, directors_name = [], []
        for person in raw_data["persons"]:
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
                    directors.append(
                        Person(
                            id=person["person_id"],
                            name=person["person_name"]
                        )
                    )
                    directors_name.append(person["person_name"])
        return FilmWorkES(
            id=raw_data["id"],
            imdb_rating=raw_data["rating"],
            genre=raw_data["genres"],
            title=raw_data["title"],
            description=raw_data["description"],
            directors=directors,
            actors_names=actors_name,
            writers_names=writers_name,
            directors_name=directors_name,
            actors=actors,
            writers=writers,
        ).dict()
