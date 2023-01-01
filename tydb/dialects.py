from typing import Any, Dict, Optional, Type, Union

import pypika
import pypika.functions
import pypika.terms

from .fields import BoolField, DateTimeField, FloatField, IntField, Nullable, StrField
from .models import Field


class Dialect:
    """
    Metadata for a database dialect.
    """

    query_builder: Type[pypika.Query] = pypika.Query

    column_types: Dict[Type[Field[Any]], str] = {
        IntField: "INTEGER",
        FloatField: "FLOAT",
        BoolField: "BOOLEAN",
        StrField: "TEXT",
    }

    datetime_default_now: Optional[Union[str, pypika.terms.Term]] = None

    @classmethod
    def column_type(cls, field: Field[Any]) -> str:
        non_null = Nullable.non_null_type(field.__class__)
        return cls.column_types[non_null]


class SQLiteDialect(Dialect):
    """
    Metadata for SQLite database connections.
    """

    query_builder = pypika.SQLLiteQuery

    # SQLite is typeless, so these types act mainly as a description of the expected type.
    column_types = {
        **Dialect.column_types,
        DateTimeField: "TIMESTAMP",
    }

    datetime_default_now = pypika.functions.CurTimestamp()  # UTC


class PostgreSQLDialect(Dialect):
    """
    Metadata for PostgreSQL database connections.
    """

    query_builder = pypika.PostgreSQLQuery

    column_types = {
        **Dialect.column_types,
        DateTimeField: "TIMESTAMP WITH TIME ZONE",
    }

    @classmethod
    def column_type(cls, field: Field[Any]) -> str:
        if isinstance(field, IntField) and field is field.owner.meta.primary:
            return "SERIAL"
        else:
            return super().column_type(field)

    datetime_default_now = pypika.functions.Now()  # Host's timezone


class MySQLDialect(Dialect):
    """
    Metadata for MySQL database connections.
    """

    query_builder = pypika.MySQLQuery

    column_types = {
        **Dialect.column_types,
        DateTimeField: "DATETIME(6)",  # Millisecond precision
    }

    @classmethod
    def column_type(cls, field: Field[Any]) -> str:
        if isinstance(field, IntField) and field is field.owner.meta.primary:
            return "INTEGER AUTO_INCREMENT"
        elif isinstance(field, StrField) and field.size:
            return "VARCHAR({})".format(field.size)
        else:
            return super().column_type(field)

    datetime_default_now = pypika.functions.CurTimestamp()  # Host's timezone
