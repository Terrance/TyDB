"""
Different database servers use different names for field types, or use special syntax for primary
keys and their types.  Each `Dialect` encapsulates this metadata for one database type; it can also
be subclassed to add support for alternative databases where the base class is insufficient.
"""

from typing import Any, Dict, Optional, Type

import pypika
import pypika.dialects
import pypika.functions
import pypika.terms

from .fields import BoolField, DateTimeField, FloatField, IntField, Nullable, StrField
from .models import Field


class Dialect:
    """
    Metadata for a database dialect.
    """

    query_builder: Type[pypika.Query] = pypika.Query
    """Specialisation of `pypika.Query`, if one exists for the database type."""

    column_types: Dict[Type[Field[Any]], str] = {
        IntField: "INTEGER",
        FloatField: "FLOAT",
        BoolField: "BOOLEAN",
        StrField: "TEXT",
    }
    """Mapping of `Field` subclasses to the names of their underlying database column types."""

    datetime_default_now: Optional[pypika.terms.Term] = None
    """Database function to retrieve the current timestamp, used by `DateTimeField`."""

    server_default: Optional[pypika.terms.Term] = pypika.terms.LiteralValue("DEFAULT")
    """Keyword used to indicate a value should use a column default."""

    @classmethod
    def column_type(cls, field: Field[Any]) -> str:
        """
        Retrieve the name of the underlying column type for a given field.  This can be overridden
        to specialise the behaviour beyond matching field types (e.g. if primary keys require a
        different type).
        """
        non_null = Nullable.non_null_type(field.__class__)
        return cls.column_types[non_null]


class _SQLiteQuery(pypika.dialects.SQLLiteQuery):

    @classmethod
    def _builder(cls, **kwargs: Any) -> pypika.dialects.SQLLiteQueryBuilder:
        return _SQLiteQueryBuilder(**kwargs)


class _SQLiteQueryBuilder(pypika.dialects.SQLLiteQueryBuilder):

    QUERY_CLS = _SQLiteQuery

    def _values_sql(self, **kwargs: Any) -> str:
        # SQLite doesn't support the per-column DEFAULT keyword for values.  As we're omitting
        # fields, we may end up trying to create a row with all default values and no inputs.
        # This can be done using DEFAULT VALUES instead of the values array, but only for one row.
        if len(self._values) == 1 and not self._values[0]:
            return " DEFAULT VALUES"
        elif any(not values for values in self._values):
            raise ValueError("Can't represent multiple rows with no columns")
        else:
            return super()._values_sql(**kwargs)


class SQLiteDialect(Dialect):
    """
    Metadata for SQLite database connections.
    """

    query_builder = _SQLiteQuery

    # SQLite is typeless, so these types act mainly as a description of the expected type.
    column_types = {
        **Dialect.column_types,
        DateTimeField: "TIMESTAMP",
    }

    datetime_default_now = pypika.functions.CurTimestamp()  # UTC

    server_default = None


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
