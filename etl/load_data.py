import logging
import os
import socket
from dotenv import load_dotenv
from etl.elasticsearchsaver import ElasticsearchSaver
from etl.postgresextractor import PostgresExtractor
from etl.transform import Transform
from utils.state import JsonFileStorage


def load_from_pg(dsl: dict, es: str):
    """Основной метод загрузки данных из Postgres в ES"""
    pg = PostgresExtractor(
        dsl=dsl,
        state_storage=JsonFileStorage(file_path="state.json"),
        page_size=100
    )
    es = ElasticsearchSaver(es_host=es)
    transform = Transform()

    """ 
    Основная цепочка корутин для обработки кинопроизведений из всех таблиц,
    собирается с конца.
    """
    data_save = es.save()
    data_transform = transform.transform(data_save)
    data_extract = pg.extract(data_transform)
    ids_filter = transform.filter_ids(data_extract)

    # Пайплайн переноса данных для person и genre
    for table in ("person", "genre"):
        ids_by_related_table = pg.get_ids_by_related_table(
            related_table=table,
            target=ids_filter
        )
        pg.get_mod_data(
            table=table,
            target=ids_by_related_table
        )

    # Пайплайн переноса данных для film_work
    pg.get_mod_data(
        table="film_work",
        target=ids_filter
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
