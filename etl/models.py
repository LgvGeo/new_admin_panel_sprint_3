import uuid
from datetime import datetime
from typing import Optional

import dateutil.parser
from pydantic import BaseModel, field_validator


class Person(BaseModel):
    id: uuid.UUID
    name: str


class Movie(BaseModel):
    id: uuid.UUID
    modified: Optional[datetime]
    actors: list[Person]
    actors_names: list[str]
    description: Optional[str]
    director: list[str]
    genre: list[str]
    imdb_rating: Optional[float]
    title: str
    writers: list[Person]
    writers_names: list[str]

    @field_validator(
            'modified',
            mode='before', check_fields=False)
    @classmethod
    def str2datetime(cls, date) -> datetime:
        if date is None:
            return date
        return dateutil.parser.isoparse(str(date))
