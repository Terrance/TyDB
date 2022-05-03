from datetime import datetime
import logging
from typing import (
    Any, Generic, Iterable, Iterator, List, Optional, Tuple, Type, TypeVar, Union, cast,
)

import pypika
from pypika.queries import CreateQueryBuilder, QueryBuilder

from tydb.dialects import Dialect
from tydb.fields import Nullable

from .api import Connection, Cursor
from .models import _RefJoinSpec, _RefSpec, BoundCollection, BoundReference, Default, Field, Table


_TAny = TypeVar("_TAny")
_TTable = TypeVar("_TTable", bound=Table)
_TTableAlt = TypeVar("_TTableAlt", bound=Table)


LOG = logging.getLogger(__name__)


class _QueryResult(Generic[_TAny]):

    def __init__(self, cursor: Cursor):
        self.cursor = cursor
        self.buffer: Optional[List[_TAny]] = None
        self.iterating = False

    def __iter__(self) -> Iterator[_TAny]:
        if self.iterating:
            raise RuntimeError("Initial result iteration still in progress")
        elif self.buffer is not None:
            return iter(self.buffer)
        else:
            return self

    def __next__(self) -> _TAny:
        if self.buffer is None:
            self.iterating = True
            self.buffer = []
        row = self.cursor.fetchone()
        if row:
            item = self.transform(row)
            self.buffer.append(item)
            return item
        else:
            self.iterating = False
            raise StopIteration

    def __repr__(self) -> str:
        state = ""
        if self.iterating:
            state = " (iterating)"
        elif self.buffer is not None:
            state = " ({} items)".format(len(self.buffer))
        return "<{}: {}{}>".format(self.__class__.__name__, self.cursor, state)

    def transform(self, row: Tuple[Any, ...]) -> _TAny:
        """
        Convert a result tuple of values into the desired type.
        """
        raise NotImplementedError


class QueryResult(_QueryResult[Tuple[Any, ...]]):
    """
    Result buffer for a query.

    Can be iterated over to fetch results incrementally from the database host.  Results are
    buffered, so multiple iterations are supported.
    """

    def transform(self, row: Tuple[Any, ...]) -> Tuple[Any, ...]:
        return row


class SelectQueryResult(_QueryResult[_TTable], Generic[_TTable]):
    """
    Query result buffer that constructs table instances from rows, handling joined tables.
    """

    def __init__(self, cursor: Cursor, table: Type[_TTable], joins: List[_RefJoinSpec]):
        super().__init__(cursor)
        self.table = table
        self.joins = joins

    def _unpack(self, table: Type[_TTableAlt], result: Tuple[Any, ...]) -> Tuple[_TTableAlt, int]:
        fields = table.meta.fields
        size = len(fields)
        row = result[:size]
        data = dict(zip(fields, row))
        return (table(**data), size)

    def transform(self, row: Tuple[Any, ...]) -> _TTable:
        final, pos = self._unpack(self.table, row)
        for path, _, _ in self.joins:
            table = path[-1].table
            instance, offset = self._unpack(table, row[pos:])
            pos += offset
            target = final
            for ref in path[:-1]:
                target = getattr(target, ref.field.name)
            setattr(target, path[-1].name, instance)
        return final


class Query:
    """
    Representation of an SQL query.
    """

    pk_query: Union[QueryBuilder, CreateQueryBuilder]

    def __init__(self, cursor: Cursor, dialect: Type[Dialect]):
        self.cursor = cursor
        self.dialect = dialect

    def execute(self) -> None:
        """
        Perform the query against the database associated with the provided cursor.
        """
        LOG.debug(self.pk_query)
        self.cursor.execute(str(self.pk_query))


class _TableQuery(Query, Generic[_TTable]):

    def __init__(self, cursor: Cursor, dialect: Type[Dialect], table: Type[_TTable]):
        super().__init__(cursor, dialect)
        self.table = table


class CreateTableQuery(_TableQuery[Table]):
    """
    Representation of a `CREATE TABLE` SQL query.
    """

    def __init__(self, cursor: Cursor, dialect: Type[Dialect], table: Type[_TTable]):
        super().__init__(cursor, dialect, table)
        self.pk_query = self._pk_query()

    def _pk_query(self):
        query: CreateQueryBuilder = (
            self.dialect.query_builder
            .create_table(self.table.meta.pk_table)
            .if_not_exists()
        )
        cols: List[pypika.Column] = []
        primary: Optional[pypika.Column] = None
        for field in self.table.meta.fields.values():
            type_ = self.dialect.column_type(field)
            nullable = Nullable.is_nullable(field.__class__)
            default = None
            if field.default is Default.TIMESTAMP_NOW:
                default = self.dialect.datetime_default_now
            elif field.default is not Default.NONE:
                default = field.default
            col = pypika.Column(field.name, type_, nullable, default)
            cols.append(col)
            if self.table.meta.primary is field:
                primary = col
        query = query.columns(*cols)
        if primary:
            query = query.primary_key(primary)
        return query


class SelectQuery(_TableQuery[_TTable]):
    """
    Representation of a `SELECT` SQL query.
    """

    def __init__(
        self, cursor: Cursor, dialect: Type[Dialect], table: Type[_TTable],
        where: Optional[pypika.Criterion] = None, *refs: _RefSpec,
    ):
        super().__init__(cursor, dialect, table)
        self.where = where
        self.joins = table.meta.join_refs(*refs)
        self.pk_query = self._pk_query()

    def _pk_query(self):
        query: QueryBuilder = (
            self.dialect.query_builder
            .from_(self.table.meta.pk_table)
            .select(*self.table.meta.fields)
        )
        for path, pk_foreign, join in self.joins:
            ref = path[-1]
            query = query.join(pk_foreign).on(join)
            cols = (pk_foreign[field] for field in ref.table.meta.fields)
            query = query.select(*cols)
        if self.where:
            query = query.where(self.where)
        return query

    def _unpack(self, table: Type[_TTableAlt], result: Tuple[Any, ...]) -> Tuple[_TTableAlt, int]:
        fields = table.meta.fields
        size = len(fields)
        row = result[:size]
        data = dict(zip(fields, row))
        return (table(**data), size)

    def execute(self) -> SelectQueryResult[_TTable]:
        """
        Like `Query.execute`, but yields the results as instances of the table's class.
        """
        super().execute()
        return SelectQueryResult(self.cursor, self.table, self.joins)


class SelectOneQuery(SelectQuery[_TTable]):
    """
    Modified `SELECT` query to apply `LIMIT 1` and fetch a single result.
    """

    def _pk_query(self):
        return super()._pk_query().limit(1)

    def execute(self) -> Optional[_TTable]:
        """
        Run `Query.execute` to completion, returning the only result if present, and `None` if not.
        """
        return next(super().execute(), None)


class InsertQuery(_TableQuery[_TTable]):
    """
    Representation of an `INSERT` SQL query.
    """

    def __init__(
        self, cursor: Cursor, dialect: Type[Dialect], table: Type[_TTable], *rows: Iterable[Any],
        fields: Optional[Iterable["Field[Any]"]] = None,
    ):
        super().__init__(cursor, dialect, table)
        self.rows = rows
        self.fields = fields or table.meta.fields.values()
        self.pk_query = self._pk_query()

    def _pk_query(self):
        cols = (field.name for field in self.fields)
        return (
            self.dialect.query_builder
            .into(self.table.meta.pk_table)
            .columns(*cols)
            .insert(*self.rows)
        )

    def execute(self) -> Optional[int]:
        """
        Run `Query.execute` to completion, and return the new primary key value if present.

        As `INSERT` queries do not return any rows, this method instead looks for the last row ID on
        the cursor, which may or may not be present, and in any case will only be available when
        inserting a single row.
        """
        super().execute()
        return self.cursor.lastrowid if self.cursor.lastrowid not in (None, -1) else None


class Session:
    """
    Wrapper around a DB-API-compatible `Connection`.
    """

    def __init__(self, conn: Connection, dialect: Type[Dialect] = Dialect):
        self.conn = conn
        self.dialect = dialect

    def __repr__(self):
        return "<{}: {!r} {}>".format(self.__class__.__name__, self.conn, self.dialect.__name__)

    def setup(self, *tables: Type[Table]):
        """
        Perform `CREATE TABLE` queries for the given tables.
        """
        for table in tables:
            cur = self.conn.cursor()
            query = CreateTableQuery(cur, self.dialect, table)
            query.execute()

    def select(
        self, table: Union[Type[_TTable], BoundCollection[_TTable]],
        where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> SelectQueryResult[_TTable]:
        """
        Perform a `SELECT` query against the given table or collection.
        """
        if isinstance(table, BoundCollection):
            bind = table
            table = cast(Type[_TTable], table.coll.ref.owner)
            assert bind.coll.ref.field.foreign
            relate = +bind.coll.ref.field == getattr(bind.inst, bind.coll.ref.field.foreign.name)
            where = relate & where if where else relate
        if auto_join:
            joins = tuple(table.meta.walk_refs())
        cur = self.conn.cursor()
        query = SelectQuery(cur, self.dialect, table, where, *joins)
        return query.execute()

    def get(
        self, table: Type[_TTable], where: Optional[pypika.Criterion] = None, *joins: _RefSpec,
        auto_join: bool = False
    ) -> Optional[_TTable]:
        """
        Perform a `SELECT ... LIMIT 1` query against the given table.

        Returns the first matching record, or `None` if there are no matches.
        """
        if auto_join:
            joins = tuple(table.meta.walk_refs())
        cur = self.conn.cursor()
        query = SelectOneQuery(cur, self.dialect, table, where, *joins)
        return query.execute()

    def load(
        self, bind: BoundReference[_TTable], *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        """
        Perform a `SELECT ... LIMIT 1` query for a referenced object.
        """
        assert bind.ref.field.foreign
        where = +bind.ref.field.foreign == getattr(bind.inst, bind.ref.field.name)
        if auto_join:
            joins = tuple(bind.ref.table.meta.walk_refs())
        cur = self.conn.cursor()
        query = SelectOneQuery(
            cur, self.dialect, cast(Type[_TTable], bind.ref.table), where, *joins,
        )
        return query.execute()

    def create(self, table: Type[_TTable], **data: Any) -> Optional[_TTable]:
        """
        Perform an `INSERT` query to add a new record to the given table.

        Returns the new instance if the underlying connection provides the last row ID.
        """
        fields: List[Field[Any]] = []
        row = []
        for name, field in table.meta.fields.items():
            try:
                value = data[name]
            except KeyError:
                if field.default is Default.NONE:
                    raise
                elif field.default is Default.SERVER:
                    continue
                elif field.default is Default.TIMESTAMP_NOW:
                    value = datetime.now().astimezone()
                else:
                    value = field.default
            row.append(value)
            fields.append(field)
        cur = self.conn.cursor()
        query = InsertQuery(cur, self.dialect, table, row, fields=fields)
        primary = query.execute()
        if primary is not None and table.meta.primary:
            return self.get(table, +table.meta.primary == primary)
