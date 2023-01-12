from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID


class Genre(BaseModel):
    id: UUID
    name: str
    description: str


class Person(BaseModel):
    id: UUID
    name: str


class Movie(BaseModel):
    id: UUID
    imdb_rating: float
    genre: List[dict]
    title: str
    description: str
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Person]
    writers: List[Person]
    directors: List[Person]
