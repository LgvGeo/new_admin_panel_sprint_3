
import redis
import redis.exceptions
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from common import LOADING_SIZE, SELECT_MOVIES_SQL_PATTERN
from models import Movie


class ElasticsearchController:
    def __init__(self, elasticsearch_conn_uri):
        self.client = Elasticsearch(elasticsearch_conn_uri)

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
        bulk(self.client, actions)


class PGController:
    def __init__(self, connection):
        self.connection = connection

    def extract_data(self, timestamp: str):
        cursor = self.connection.cursor()
        select_movies_sql_stmt = SELECT_MOVIES_SQL_PATTERN.format(
            timestamp=timestamp)
        cursor.execute(select_movies_sql_stmt)
        while True:
            data = [
                Movie(**x) for x in cursor.fetchmany(LOADING_SIZE)
            ]
            if not data:
                break
            yield data


class RedisController:

    def __init__(self, connection: redis.Redis):
        self.connection = connection

    def save_state(self, key, value):
        return self.connection.hset('storage', key, value)

    def retrieve_state(self, key):
        return self.connection.hget('storage', key)
