import datetime
import logging
import psycopg2
from utils.backoff import backoff
from utils.coroutine import coroutine
from utils.state import State, BaseStorage
from psycopg2.extras import DictCursor
from psycopg2.extensions import cursor
from collections.abc import Coroutine


class PostgresExtractor:
    """Класс используется для выгрузки данных из Postgres"""

    def __init__(self, dsl: dict, state_storage: BaseStorage, page_size=100):
        self.connection = None
        self.dsl = dsl
        self.page_size = page_size
        self.state = State(storage=state_storage)

    def connect(self):
        self.connection = psycopg2.connect(**self.dsl, connect_timeout=3)

    def disconnect(self):
        if self.connection and not self.connection.closed:
            self.connection.close()

    def cursor(self) -> cursor:
        if not self.connection or self.connection.closed:
            self.connect()
        return self.connection.cursor(cursor_factory=DictCursor)

    @backoff(
        exceptions=(psycopg2.OperationalError, psycopg2.DatabaseError)
    )
    def execute(self, query: str, data: tuple) -> list:
        """Получение данных из Postgres с контролем состояния"""
        with self.cursor() as cur:
            cur.execute(query, data)
            return cur.fetchall()

    def key_state(self, table: str) -> str:
        """Формирования ключа для сохранения состояния"""
        res = "_last_modified"
        if table:
            res = table + res
        return res

    def get_mod_data(self, target: Coroutine[None, list, None], table: str):
        """Выгрузка обновленных id из таблицы table"""
        last_modified = self.state.get_state(self.key_state(table))
        if not last_modified:
            last_modified = datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)

        query = """
            SELECT id, modified
            FROM content.{0}
            WHERE modified > %s
            ORDER BY modified
            LIMIT %s
        """.format(
            table
        )

        while True:
            logging.info(
                "Загрузка ID для таблицы {0}, начиная с {1}".format(
                    table, last_modified
                )
            )
            data = (last_modified, self.page_size)
            table_data = self.execute(query, data)

            if len(table_data) == 0:
                break

            logging.info("Загружено ID: {0}".format(len(table_data)))

            target.send(table_data)

            last_modified = table_data[-1]["modified"]
            self.state.set_state(
                key=self.key_state(table),
                value=str(last_modified.replace(tzinfo=datetime.timezone.utc)),
            )

    @coroutine
    def get_ids_by_related_table(self, related_table: str, target: Coroutine[None, list, None]):
        """Выгрузка id кинопроизведений из таблиц связей других сущностей (person, genre)"""
        query = """
            SELECT DISTINCT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.{0}_film_work pfw ON pfw.film_work_id = fw.id
            WHERE pfw.{0}_id IN %s and fw.id > %s
            ORDER BY fw.id
            LIMIT %s
        """.format(
            related_table
        )

        while related_data := (yield):
            related_ids = tuple(x["id"] for x in related_data)
            last_id = "00000000-0000-0000-0000-000000000000"
            while True:
                data = (related_ids, last_id, self.page_size)
                film_work_ids = self.execute(query, data)

                if len(film_work_ids) == 0:
                    break

                logging.info(
                    "Загружено ID кинопроизведений связанных с {0}: {1}".format(
                        related_table, len(film_work_ids)
                    )
                )

                last_id = film_work_ids[-1]["id"]
                target.send(film_work_ids)

    @coroutine
    def extract(self, target: Coroutine[None, list, None]):
        """Выгрузка всех данных кинопроизведений"""
        query = """
            SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating as imdb_rating,
            fw.type,
            COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'role', pfw.role,
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null),
                '[]'
            ) as persons,
            array_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s
            GROUP BY fw.id
        """

        while ids := (yield):
            res = self.execute(query, (ids,))
            target.send(res)
