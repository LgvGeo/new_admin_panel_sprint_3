
import redis
import redis.exceptions
from elasticsearch.helpers import bulk
from pydantic import BaseModel

from common import (LOADING_SIZE, SELECT_GENRES_SQL_PATTERN,
                    SELECT_MOVIES_SQL_PATTERN, SELECT_PERSONS_SQL_PATTERN)
from models import Genre, Movie, Person


class ElasticsearchController:
    def __init__(self, connection):
        self.connection = connection

    def _load(self, documents: list[BaseModel], index):
        actions = []
        for document in documents:
            dict_format = document.model_dump(exclude=['modified'])
            obj = {
                "_index": index,
                "_id": dict_format['id'],
                "_source": dict_format
            }
            actions.append(obj)
        bulk(self.connection, actions)

    def load_movies(self, documents: list[Movie]):
        self._load(documents, 'movies')

    def load_genres(self, documents: list[Genre]):
        self._load(documents, 'genres')

    def load_persons(self, documents: list[Person]):
        self._load(documents, 'persons')

    def close(self):
        self.connection.close()


class PGController:
    def __init__(self, connection):
        self.connection = connection

    def _extract(self, timestamp: str, model: BaseModel, sql_pattern):
        with self.connection.cursor() as cursor:
            sql_stmt = sql_pattern.format(
                timestamp=timestamp)
            cursor.execute(sql_stmt)
            while rows := cursor.fetchmany(LOADING_SIZE):
                data = [
                    model(**x) for x in rows
                ]
                yield data

    def extract_movies(self, timestamp: str):
        for data in self._extract(timestamp, Movie, SELECT_MOVIES_SQL_PATTERN):
            yield data

    def extract_genres(self, timestamp: str):
        for data in self._extract(timestamp, Genre, SELECT_GENRES_SQL_PATTERN):
            yield data

    def extract_persons(self, timestamp: str):
        for data in self._extract(
            timestamp, Person, SELECT_PERSONS_SQL_PATTERN
        ):
            yield data


class RedisController:

    def __init__(self, connection: redis.Redis):
        self.connection = connection

    def save_state(self, key, value):
        return self.connection.hset('storage', key, value)

    def retrieve_state(self, key):
        return self.connection.hget('storage', key)
