import time
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager

import backoff
import elasticsearch.exceptions
import psycopg2
import psycopg2.errors
import redis
import redis.exceptions
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from common import SLEEPING_TIME
from controllers import ElasticsearchController, PGController, RedisController
from logger import configure_logger
from models import ElasticsearchSettings, PGSettings, RedisSettings

psycopg2.extras.register_uuid()
log = configure_logger()


@contextmanager
def postgres_connection(**kwargs):
    conn = psycopg2.connect(**kwargs, cursor_factory=DictCursor)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


@contextmanager
def redis_connection(**kwargs):
    conn = redis.Redis(**kwargs, decode_responses=True)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def elastic_connection(**kwargs):
    conn = Elasticsearch(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


@backoff.on_exception(
        backoff.expo,
        (
            psycopg2.errors.ConnectionException,
            redis.exceptions.ConnectionError,
            elasticsearch.exceptions.ConnectionError,
            psycopg2.errors.OperationalError),
        max_tries=8,
        on_backoff=log.error)
def process_movies(pg_conf, redis_conf, elasticsearch_conf):
    while True:
        with (
            redis_connection(**redis_conf) as redis_conn,
            postgres_connection(**pg_conf) as pg_conn,
            elastic_connection(**elasticsearch_conf) as elastic_conn
        ):
            pg_controller = PGController(pg_conn)
            redis_controller = RedisController(redis_conn)
            elasticsearch_controller = ElasticsearchController(
                elastic_conn)

            timestamp = (
                redis_controller.retrieve_state('movies_timestamp')
                or '-infinity'
            )
            log.info(f'last processed movies_timestamp: {timestamp}')
            for data in pg_controller.extract_movies(timestamp):
                log.info(f'num of processed records: {len(data)}')
                elasticsearch_controller.load_movies(data)
                max_timestamp_in_data = data[-1].modified.isoformat()
                redis_controller.save_state(
                    'movies_timestamp', max_timestamp_in_data)
        log.info('no movies data to process, sleep')
        time.sleep(SLEEPING_TIME)


@backoff.on_exception(
        backoff.expo,
        (
            psycopg2.errors.ConnectionException,
            redis.exceptions.ConnectionError,
            elasticsearch.exceptions.ConnectionError,
            psycopg2.errors.OperationalError),
        max_tries=8,
        on_backoff=log.error)
def process_genres(pg_conf, redis_conf, elasticsearch_conf):
    while True:
        with (
            redis_connection(**redis_conf) as redis_conn,
            postgres_connection(**pg_conf) as pg_conn,
            elastic_connection(**elasticsearch_conf) as elastic_conn
        ):
            pg_controller = PGController(pg_conn)
            redis_controller = RedisController(redis_conn)
            elasticsearch_controller = ElasticsearchController(
                elastic_conn)

            timestamp = (
                redis_controller.retrieve_state('genres_timestamp')
                or '-infinity'
            )
            log.info(f'last processed timestamp: {timestamp}')
            for data in pg_controller.extract_genres(timestamp):
                log.info(f'num of processed records: {len(data)}')
                elasticsearch_controller.load_genres(data)
                max_timestamp_in_data = data[-1].modified.isoformat()
                redis_controller.save_state(
                    'genres_timestamp', max_timestamp_in_data)
        log.info('no genres data to process, sleep')
        time.sleep(SLEEPING_TIME)


@backoff.on_exception(
        backoff.expo,
        (
            psycopg2.errors.ConnectionException,
            redis.exceptions.ConnectionError,
            elasticsearch.exceptions.ConnectionError,
            psycopg2.errors.OperationalError),
        max_tries=8,
        on_backoff=log.error)
def process_persons(pg_conf, redis_conf, elasticsearch_conf):
    while True:
        with (
            redis_connection(**redis_conf) as redis_conn,
            postgres_connection(**pg_conf) as pg_conn,
            elastic_connection(**elasticsearch_conf) as elastic_conn
        ):
            pg_controller = PGController(pg_conn)
            redis_controller = RedisController(redis_conn)
            elasticsearch_controller = ElasticsearchController(
                elastic_conn)

            timestamp = (
                redis_controller.retrieve_state('persons_timestamp')
                or '-infinity'
            )
            log.info(f'last processed timestamp: {timestamp}')
            for data in pg_controller.extract_persons(timestamp):
                log.info(f'num of processed records: {len(data)}')
                elasticsearch_controller.load_persons(data)
                max_timestamp_in_data = data[-1].modified.isoformat()
                redis_controller.save_state(
                    'persons_timestamp', max_timestamp_in_data)
        log.info('no persons data to process, sleep')
        time.sleep(SLEEPING_TIME)


if __name__ == '__main__':
    pg_conf = PGSettings().model_dump()
    redis_conf = RedisSettings().model_dump()
    elasticsearch_conf = ElasticsearchSettings().model_dump()
    tasks_list = []
    with ThreadPoolExecutor() as pool:
        task = pool.submit(
            process_movies, pg_conf, redis_conf, elasticsearch_conf)
        tasks_list.append(task)
        task = pool.submit(
            process_genres, pg_conf, redis_conf, elasticsearch_conf)
        tasks_list.append(task)
        task = pool.submit(
            process_persons, pg_conf, redis_conf, elasticsearch_conf)
        tasks_list.append(task)
        wait(tasks_list)
