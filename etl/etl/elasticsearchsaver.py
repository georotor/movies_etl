import logging
from elasticsearch import Elasticsearch, ConnectionError, ConnectionTimeout
from utils.backoff import backoff
from utils.coroutine import coroutine
from typing import Any


class ElasticsearchSaver:
    """Класс используется для обновления данных в Elasticsearch"""

    def __init__(self, es_host: str):
        self.es = Elasticsearch(es_host)

    def close(self):
        if self.es:
            self.es.close()

    @backoff(
        exceptions=(ConnectionError, ConnectionTimeout)
    )
    def bulk(self, index: str, operations: list) -> Any:
        """Отправка данных в Elasticsearch с контролем состояния"""
        return self.es.bulk(
            index=index, operations=operations, filter_path="items.*.error"
        )

    @coroutine
    def save(self):
        """Корутина для пакетной загрузки данных в Elasticsearch"""
        while data := (yield):
            error = self.bulk(index="movies", operations=data)
            if error:
                logging.error("Не удалось обновить записи в Elasticsearch")
                logging.error(str(error))
