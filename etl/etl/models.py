from pydantic import BaseModel, validator
from uuid import UUID


class Genre(BaseModel):
    id: UUID
    name: str
    description: str
    films_count: int


class GenreShort(BaseModel):
    id: UUID
    name: str


class Person(BaseModel):
    id: UUID
    name: str


class Movie(BaseModel):
    id: UUID
    imdb_rating: float = 0.0
    genre: list[GenreShort]
    title: str
    description: str
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]

    @validator('imdb_rating', pre=True)
    def check_imdb_rating(cls, v):
        return v or 0.0
