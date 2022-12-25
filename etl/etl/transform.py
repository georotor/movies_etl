import logging
from collections.abc import Coroutine
from utils.coroutine import coroutine
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


class Transform:
    """Класс подготовки данных"""

    def __init__(self, logger=None):
        self.filtred = set()

    @coroutine
    def filter_ids(self, target: Coroutine[None, tuple, None]):
        """Фильтр по id + modified, исключающий кинопроизведения,
        которые уже были загружены в течении текущего сеанса"""

        while rows := (yield):
            ids = set((x["id"], x["modified"]) for x in rows)
            res = ids - self.filtred
            logging.info(
                "Фильтр: получил {0}, отфильтровал: {1}, осталось {2}".format(
                    len(rows), len(ids) - len(res), len(res)
                )
            )
            self.filtred.update(res)

            if len(res) > 0:
                target.send(tuple(x[0] for x in res))

    @coroutine
    def transform(self, target: Coroutine[None, list, None]):
        """Подготовка данных для bulk запроса в Elasticsearch"""
        while rows := (yield):
            data = []
            for row in rows:
                action = {"index": {"_index": "movies", "_id": row["id"]}}
                movie = Movie(
                    id=row["id"],
                    imdb_rating=row["imdb_rating"] if row["imdb_rating"] else 0,
                    genre=row["genres"] if row["genres"][0] else [],
                    title=row["title"],
                    description=row["description"],
                    **self.persons_parse(row["persons"]),
                )

                data.append(action)
                data.append(movie.dict())

            target.send(data)

    def persons_parse(self, persons: list) -> dict:
        """Парсинг персон кинопроизведения"""
        res = {
            "director": [],
            "actors_names": [],
            "writers_names": [],
            "actors": [],
            "writers": [],
        }
        for person in persons:
            match person["role"]:
                case "director":
                    res["director"].append(person["name"])
                case "writer":
                    res["writers_names"].append(person["name"])
                    res["writers"].append({"id": person["id"], "name": person["name"]})
                case "actor":
                    res["actors_names"].append(person["name"])
                    res["actors"].append({"id": person["id"], "name": person["name"]})

        return res
