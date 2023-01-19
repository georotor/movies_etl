import logging
from collections.abc import Coroutine
from etl.models import Movie, Person, Genre
from utils.coroutine import coroutine


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
    def transform(self, index: str, model: Movie | Person | Genre, target: Coroutine[None, list, None]):
        """Подготовка данных для bulk запроса в Elasticsearch"""
        while rows := (yield):
            data = []
            for row in rows:
                action = {"index": {"_index": index, "_id": row["id"]}}
                _model = model(**row)

                data.append(action)
                data.append(_model.dict())

            target.send(data)
