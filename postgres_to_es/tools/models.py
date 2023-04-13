from enum import Enum
from uuid import UUID

from pydantic import BaseModel


class FilmType(Enum):
    tv_show = "tv_show"
    movie = "movie"


class PersonType(Enum):
    actor = "actor"
    writer = "writer"
    director = "director"


class UUIDMixin(BaseModel):
    id: UUID


class Person(UUIDMixin):
    name: str


class FilmWorkES(UUIDMixin):
    title: str
    imdb_rating: float | None
    genre: list[str] | None
    description: str | None
    director: list[str] | None
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]