
import redis
import redis.exceptions
from elasticsearch.helpers import bulk

from common import LOADING_SIZE, SELECT_MOVIES_SQL_PATTERN
from models import Movie


class ElasticsearchController:
    def __init__(self, connection):
        self.connection = connection

    def load_movies(self, documents: list[Movie]):
        index = 'movies'
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

    def close(self):
        self.connection.close()


class PGController:
    def __init__(self, connection):
        self.connection = connection

    def extract_data(self, timestamp: str):
        with self.connection.cursor() as cursor:
            select_movies_sql_stmt = SELECT_MOVIES_SQL_PATTERN.format(
                timestamp=timestamp)
            cursor.execute(select_movies_sql_stmt)
            while rows := cursor.fetchmany(LOADING_SIZE):
                data = [
                    Movie(**x) for x in rows
                ]
                yield data


class RedisController:

    def __init__(self, connection: redis.Redis):
        self.connection = connection

    def save_state(self, key, value):
        return self.connection.hset('storage', key, value)

    def retrieve_state(self, key):
        return self.connection.hget('storage', key)
