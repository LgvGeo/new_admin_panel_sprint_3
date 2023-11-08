import uuid
from datetime import datetime
from typing import Optional

import dateutil.parser
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


class PGSettings(BaseSettings):
    dbname: str = Field(default='postgres', validation_alias='DB_NAME')
    user: str = Field(default='postgres', validation_alias='DB_USER')
    password: str = Field(default='postgres', validation_alias='DB_PASSWORD')
    host: str = Field(default='127.0.0.1', validation_alias='DB_HOST')
    port: str = Field(default='5432', validation_alias='DB_PORT')


class RedisSettings(BaseSettings):
    host: str = Field(default='127.0.0.1', validation_alias='REDIS_HOST')
    port: str = Field(default='6379', validation_alias='REDIS_PORT')


class ElasticsearchSettings(BaseSettings):
    hosts: str = Field(
        default='http://127.0.0.1:9200',
        validation_alias='ELASTICSEARCH_CONN_URI')


class PersonForMovie(BaseModel):
    id: uuid.UUID
    name: str


class GenreForMovie(BaseModel):
    id: uuid.UUID
    name: str


class Genre(BaseModel):
    id: uuid.UUID
    name: str
    modified: Optional[datetime]
    description: Optional[str]

    @field_validator(
            'modified',
            mode='before', check_fields=False)
    @classmethod
    def str2datetime(cls, date) -> datetime:
        if date is None:
            return date
        return dateutil.parser.isoparse(str(date))


class MovieForPerson(BaseModel):
    id: uuid.UUID
    title: str
    roles: list[str]


class Person(BaseModel):
    id: uuid.UUID
    name: str
    modified: Optional[datetime]
    films: list[MovieForPerson]

    @field_validator(
            'modified',
            mode='before', check_fields=False)
    @classmethod
    def str2datetime(cls, date) -> datetime:
        if date is None:
            return date
        return dateutil.parser.isoparse(str(date))


class Movie(BaseModel):
    id: uuid.UUID
    modified: Optional[datetime]
    actors: list[PersonForMovie]
    actors_names: list[str]
    description: Optional[str]
    director: list[str]
    genre: list[GenreForMovie]
    imdb_rating: Optional[float]
    title: str
    writers: list[PersonForMovie]
    writers_names: list[str]

    @field_validator(
            'modified',
            mode='before', check_fields=False)
    @classmethod
    def str2datetime(cls, date) -> datetime:
        if date is None:
            return date
        return dateutil.parser.isoparse(str(date))
