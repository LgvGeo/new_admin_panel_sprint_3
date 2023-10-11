import os
import time
from contextlib import contextmanager

import backoff
import elasticsearch.exceptions
import psycopg2
import psycopg2.errors
import redis
import redis.exceptions
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from common import SLEEPING_TIME
from controllers import ElasticsearchController, PGController, RedisController
from logger import configure_logger

load_dotenv()
psycopg2.extras.register_uuid()
log = configure_logger()


@contextmanager
def postgres_conntection(**kwargs):
    conn = psycopg2.connect(**kwargs, cursor_factory=DictCursor)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


@contextmanager
def redis_conntection(**kwargs):
    r = redis.Redis(**kwargs, decode_responses=True)
    try:
        yield r
    finally:
        r.close()


@backoff.on_exception(
        backoff.expo,
        (
            psycopg2.errors.ConnectionException,
            redis.exceptions.ConnectionError,
            elasticsearch.exceptions.ConnectionError,
            psycopg2.errors.OperationalError),
        max_tries=8,
        on_backoff=log.error)
def process_movies(pg_conf, redis_conf, elasticsearch_conn_uri):
    while True:
        with (
            redis_conntection(**redis_conf) as redis_conn,
            postgres_conntection(**pg_conf) as pg_conn
        ):
            pg_controller = PGController(pg_conn)
            redis_controller = RedisController(redis_conn)
            elasticsearch_controller = ElasticsearchController(
                elasticsearch_conn_uri)

            timestamp = (
                redis_controller.retrieve_state('timestamp')
                or '-infinity'
            )
            log.info(f'last processed timestamp: {timestamp}')
            for data in pg_controller.extract_data(timestamp):
                log.info(f'num of processed records: {len(data)}')
                elasticsearch_controller.load_movies(data)
                max_timestamp_in_data = data[-1].modified.isoformat()
                redis_controller.save_state('timestamp', max_timestamp_in_data)
        log.info('no data to process, sleep')
        time.sleep(SLEEPING_TIME)


if __name__ == '__main__':
    pg_conf = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT')
    }
    redis_conf = {
        'host': os.environ.get('REDIS_HOST', '127.0.0.1'),
        'port': os.environ.get('REDIS_PORT', '6379'),
    }
    elasticsearch_conn_uri = os.environ.get('ELASTICSEARCH_CONN_URI')
    process_movies(pg_conf, redis_conf, elasticsearch_conn_uri)
