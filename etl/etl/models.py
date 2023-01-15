from pydantic import BaseModel
from uuid import UUID


class Genre(BaseModel):
    id: UUID
    films_count: int
    name: str
    description: str


class Person(BaseModel):
    id: UUID
    name: str


class Movie(BaseModel):
    id: UUID
    imdb_rating: float
    genre: list[dict]
    title: str
    description: str
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]
