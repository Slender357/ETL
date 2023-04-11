from typing import Iterable, Any
from etl.tools.models import FilmWorkES, PersonType, Person


class Transform:
    def __init__(self, iterator: Iterable[dict[str, Any]]):
        self.iterator = iterator

    def transform(self) -> Iterable[dict[str, Any]]:
        for row in self.iterator:
            movie = self._pre_validate(row).dict()
            movie["_id"] = movie["id"]
            yield movie

    @staticmethod
    def _pre_validate(row: dict[str, Any]) -> FilmWorkES:
        actors, actors_name = [], []
        writers, writers_name = [], []
        director = []
        for person in row["persons"]:
            match person["person_role"]:
                case PersonType.actor.value:
                    actors.append(
                        Person(id=person["person_id"], name=person["person_name"])
                    )
                    actors_name.append(person["person_name"])
                case PersonType.writer.value:
                    writers.append(
                        Person(id=person["person_id"], name=person["person_name"])
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
        )
