import logging
import os
import socket
from dotenv import load_dotenv
from etl.elasticsearchsaver import ElasticsearchSaver
from etl.models import Genre, Person, Movie
from etl.postgresextractor import PostgresExtractor
from etl.transform import Transform
from etl.sqlselect import SELECT_MOVIES, SELECT_GENRES, SELECT_PERSONS
from utils.state import JsonFileStorage


def load_from_pg(dsl: dict, es: str):
    """Основной метод загрузки данных из Postgres в ES"""
    pg = PostgresExtractor(
        dsl=dsl,
        state_storage=JsonFileStorage(file_path="state.json"),
        page_size=int(os.environ.get("PAGE_SIZE", 100))
    )
    es = ElasticsearchSaver(es_host=es)
    transform = Transform()

    # Корутина для записи данных в ES
    data_save = es.save()

    """ 
    Цепочка корутин для обработки кинопроизведений из всех таблиц, собирается с конца.
    """
    movies_transform = transform.transform(index="movies", model=Movie, target=data_save)
    movies_extract = pg.extract(
        query=SELECT_MOVIES,
        target=movies_transform
    )
    movies_filter = transform.filter_ids(movies_extract)

    """
    Цепочка корутин для обработки жанров
    """
    genres_transform = transform.transform(index="genres", model=Genre, target=data_save)
    genres_extract = pg.extract(query=SELECT_GENRES, target=genres_transform)

    """
    Цепочка корутин для обработки персон
    """
    persons_transform = transform.transform(index="persons", model=Person, target=data_save)
    persons_extract = pg.extract(query=SELECT_PERSONS, target=persons_transform)

    """
    Пайплайн переноса данных для person и genre
    """
    for table, pipeline in (("genre", genres_extract), ("person", persons_extract)):
        ids_by_related_table = pg.get_ids_by_related_table(
            related_table=table,
            target=movies_filter
        )
        # Ищем обновленные строки и запускаем пайплайны для самой таблицы и связанных кинопроизведений
        pg.get_mod_data(
            table=table,
            target=(pipeline, ids_by_related_table)
        )

    """
    Пайплайн переноса данных для film_work
    """
    pg.get_mod_data(
        table="film_work",
        target=(movies_filter,)
    )

    pg.disconnect()
    es.close()


def is_run():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', 64396))
            return False
    except OSError:
        return True


def main():
    if is_run():
        logging.warning("Уже работаем")
        exit()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 64396))

        load_dotenv()

        dsl = {
            "dbname": os.environ.get("POSTGRES_DB"),
            "user": os.environ.get("POSTGRES_USER"),
            "password": os.environ.get("POSTGRES_PASSWORD"),
            "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            "port": os.environ.get("POSTGRES_PORT", 5432),
        }
        es = os.environ.get("ES_HOST", "http://localhost:9200")

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )

        load_from_pg(dsl, es)


if __name__ == "__main__":
    main()
