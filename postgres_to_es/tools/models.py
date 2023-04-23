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
    directors_name: list[str] | None
    actors_names: list[str] | None
    writers_names: list[str] | None
    actors: list[Person]
    directors: list[Person]
    writers: list[Person]


class PersonES(UUIDMixin):
    full_name: str


class GenresES(UUIDMixin):
    name: str
    description: str | None
