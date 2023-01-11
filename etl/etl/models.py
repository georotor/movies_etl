from pydantic import BaseModel
from typing import List
from uuid import UUID

class Person(BaseModel):
    id: UUID
    name: str


class Movie(BaseModel):
    id: UUID
    imdb_rating: float
    genre: List[str]
    title: str
    description: str
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Person]
    writers: List[Person]


class Genre(BaseModel):
    id: UUID
    name: str
    description: str