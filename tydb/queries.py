from collections.abc import Awaitable
import logging
from typing import (
    Any, AsyncIterator, Callable, Generic, Iterable, Iterator, List, Optional, Tuple, Type, TypeVar,
    Union, overload,
)

import pypika
from pypika.queries import CreateQueryBuilder, QueryBuilder
from typing_extensions import Self

from .api import AsyncCursor, Cursor
from .dialects import Dialect
from .fields import Nullable
from .models import _RefJoinSpec, _RefSpec, Default, Field, Table
from .utils import maybe_await


_TAny = TypeVar("_TAny")
_TAnyAlt = TypeVar("_TAnyAlt")
_TTable = TypeVar("_TTable", bound=Table)
_TTableAlt = TypeVar("_TTableAlt", bound=Table)


LOG = logging.getLogger(__name__)


class _AsyncIterProxy(Iterator[_TAny], AsyncIterator[_TAny]):

    def __init__(self, iterator: Iterable[_TAny]):
        self.iter = iter(iterator)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def __next__(self):
        return next(self.iter)

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


class _CommonQueryResult(Generic[_TAny]):

    def __init__(self):
        self.buffer: List[_TAny] = []
        self.iterating = False
        self.done = False

    def _iter(self, iterator: Callable[[List[_TAny]], _TAnyAlt]) -> Union[Self, _TAnyAlt]:
        if self.iterating:
            raise RuntimeError("Initial result iteration still in progress")
        elif self.done:
            return iterator(self.buffer)
        else:
            return self

    def _next_before(self):
        if not self.iterating:
            self.iterating = True

    def _next_after(self, row: Optional[Tuple[Any, ...]]) -> _TAny:
        if row:
            item = self.transform(row)
            self.buffer.append(item)
            return item
        else:
            self.iterating = False
            self.done = True
            raise StopIteration

    def __repr__(self) -> str:
        if self.done:
            state = repr(self.buffer)
        elif self.iterating:
            state = "iterating"
        else:
            state = "not iterated"
        return "<{}: {}>".format(self.__class__.__name__, state)

    def transform(self, row: Tuple[Any, ...]) -> _TAny:
        """
        Convert a result tuple of values into the desired type.
        """
        raise NotImplementedError


class _QueryResult(_CommonQueryResult[_TAny]):

    def __init__(self, cursor: Cursor):
        super().__init__()
        self.cursor = cursor

    def __iter__(self) -> Iterator[_TAny]:
        return self._iter(iter)

    def __next__(self) -> _TAny:
        self._next_before()
        row = self.cursor.fetchone()
        return self._next_after(row)

    def __aiter__(self) -> AsyncIterator[_TAny]:
        return self._iter(_AsyncIterProxy)

    async def __anext__(self) -> _TAny:
        self._next_before()
        row = await maybe_await(self.cursor.fetchone())
        try:
            return self._next_after(row)
        except StopIteration:
            raise StopAsyncIteration


class _AsyncQueryResult(_CommonQueryResult[_TAny]):

    def __init__(self, cursor: AsyncCursor):
        super().__init__()
        self.cursor = cursor

    def __aiter__(self) -> AsyncIterator[_TAny]:
        return self._iter(_AsyncIterProxy)

    async def __anext__(self) -> _TAny:
        self._next_before()
        row = await maybe_await(self.cursor.fetchone())
        try:
            return self._next_after(row)
        except StopIteration:
            raise StopAsyncIteration


class _RawQueryResult(_CommonQueryResult[Tuple[Any, ...]]):
    """
    Result buffer for a query.

    Can be iterated over to fetch results incrementally from the database host.  Results are
    buffered, so multiple iterations are supported.
    """

    def transform(self, row: Tuple[Any, ...]) -> Tuple[Any, ...]:
        return row


class RawQueryResult(_RawQueryResult, _QueryResult[Tuple[Any, ...]]):
    pass


class AsyncRawQueryResult(_RawQueryResult, _AsyncQueryResult[Tuple[Any, ...]]):
    pass


class _SelectQueryResult(_CommonQueryResult[_TTable]):
    """
    Query result buffer that constructs table instances from rows, handling joined tables.
    """

    def __init__(self, table: Type[_TTable], joins: List[_RefJoinSpec]):
        _CommonQueryResult.__init__(self)
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


class SelectQueryResult(_SelectQueryResult, _QueryResult[_TTable]):
    
    def __init__(self, cursor: Cursor, table: Type[_TTable], joins: List[_RefJoinSpec]):
        _QueryResult.__init__(self, cursor)
        _SelectQueryResult.__init__(self, table, joins)


class AsyncSelectQueryResult(_SelectQueryResult, _AsyncQueryResult[_TTable]):
    
    def __init__(self, cursor: AsyncCursor, table: Type[_TTable], joins: List[_RefJoinSpec]):
        _AsyncQueryResult.__init__(self, cursor)
        _SelectQueryResult.__init__(self, table, joins)


class _Query(Generic[_TTable]):

    pk_query: Union[QueryBuilder, CreateQueryBuilder]

    def __init__(self, dialect: Type[Dialect], table: Type[_TTable]):
        self.dialect = dialect
        self.table = table

    @overload
    def execute(self, cursor: Cursor) -> None: ...
    @overload
    def execute(self, cursor: AsyncCursor) -> Awaitable[None]: ...
    
    def execute(self, cursor: Union[Cursor, AsyncCursor]):
        """
        Perform the query against the database associated with the provided cursor.
        """
        LOG.debug(self.pk_query)
        return cursor.execute(str(self.pk_query))


class CreateTableQuery(_Query[Table]):
    """
    Representation of a `CREATE TABLE` SQL query.
    """

    def __init__(self, dialect: Type[Dialect], table: Type[_TTable]):
        super().__init__(dialect, table)
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
            elif not isinstance(field.default, Default):
                default = field.default
            col = pypika.Column(field.name, type_, nullable, default)
            cols.append(col)
            if self.table.meta.primary is field:
                primary = col
        query = query.columns(*cols)
        if primary:
            query = query.primary_key(primary)
        return query


class DropTableQuery(_Query[Table]):
    """
    Representation of a `DROP TABLE` SQL query.
    """

    def __init__(self, dialect: Type[Dialect], table: Type[_TTable]):
        super().__init__(dialect, table)
        self.pk_query = self._pk_query()

    def _pk_query(self):
        return (
            self.dialect.query_builder
            .drop_table(self.table.meta.pk_table)
            .if_exists()
        )


class _SelectQuery(_Query[_TTable]):
    """
    Representation of a `SELECT` SQL query.
    """

    def __init__(
        self, dialect: Type[Dialect], table: Type[_TTable],
        where: Optional[pypika.Criterion] = None, *refs: _RefSpec,
    ):
        super().__init__(dialect, table)
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

    def execute(self, cursor: Union[Cursor, AsyncCursor]):
        """
        Like `Query.execute`, but yields the results as instances of the table's class.
        """
        return super().execute(cursor)


class SelectQuery(_SelectQuery[_TTable]):

    def execute(self, cursor: Cursor) -> SelectQueryResult[_TTable]:
        super().execute(cursor)
        return SelectQueryResult(cursor, self.table, self.joins)


class AsyncSelectQuery(_SelectQuery[_TTable]):

    async def execute(self, cursor: AsyncCursor) -> AsyncSelectQueryResult[_TTable]:
        await maybe_await(super().execute(cursor))
        return AsyncSelectQueryResult(cursor, self.table, self.joins)


class _SelectOneQuery(_SelectQuery[_TTable]):
    """
    Modified `SELECT` query to apply `LIMIT 1` and fetch a single result.
    """

    def _pk_query(self):
        return super()._pk_query().limit(1)

    def execute(self, cursor: Union[Cursor, AsyncCursor]):
        """
        Run `Query.execute` to completion, returning the only result if present, and `None` if not.
        """
        return super().execute(cursor)


class SelectOneQuery(SelectQuery[_TTable], _SelectOneQuery[_TTable]):

    def execute(self, cursor: Cursor):
        result = super().execute(cursor)
        return next(result, None)


class AsyncSelectOneQuery(AsyncSelectQuery[_TTable], _SelectOneQuery[_TTable]):
    """
    Modified `SELECT` query to apply `LIMIT 1` and fetch a single result.
    """

    async def execute(self, cursor: AsyncCursor):
        result = await super().execute(cursor)
        return await anext(result, None)


class _InsertQuery(_Query[_TTable]):
    """
    Representation of an `INSERT` SQL query.
    """

    def __init__(
        self, dialect: Type[Dialect], table: Type[_TTable], *rows: Iterable[Any],
        fields: Optional[Iterable["Field[Any]"]] = None,
    ):
        super().__init__(dialect, table)
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

    def _get_row(self, cursor: Union[Cursor, AsyncCursor]) -> Optional[int]:
        last = getattr(cursor, "lastrowid", None)
        return last if last not in (None, -1) else None

    def execute(self, cursor: Union[Cursor, AsyncCursor]):
        """
        Run `Query.execute` to completion, and return the new primary key value if present.

        As `INSERT` queries do not return any rows, this method instead looks for the last row ID on
        the cursor, which may or may not be present, and in any case will only be available when
        inserting a single row.
        """
        return super().execute(cursor)


class InsertQuery(_InsertQuery[_TTable]):

    def execute(self, cursor: Cursor):
        super().execute(cursor)
        return self._get_row(cursor)


class AsyncInsertQuery(_InsertQuery[_TTable]):

    async def execute(self, cursor: AsyncCursor):
        await super().execute(cursor)
        return self._get_row(cursor)


class DeleteQuery(_Query[_TTable]):
    """
    Representation of a `DELETE` SQL query.
    """

    def __init__(self, dialect: Type[Dialect], table: Type[_TTable], *insts: _TTable):
        if not table.meta.primary:
            raise RuntimeError("Table {} has no primary key".format(table.meta.name))
        super().__init__(dialect, table)
        self.pk_query = self._pk_query(*insts)

    def _pk_query(self, *insts: _TTable):
        ids = [
            getattr(inst, self.table.meta.primary.name) if isinstance(inst, self.table) else inst
            for inst in insts
        ]
        return (
            self.dialect.query_builder
            .from_(self.table.meta.pk_table)
            .delete()
            .where((+self.table.meta.primary).isin(ids))
        )


class DeleteOneQuery(_Query[_TTable]):
    """
    Representation of a `DELETE` SQL query that compares all fields, for tables with no primary key.
    """

    def __init__(self, dialect: Type[Dialect], inst: _TTable):
        super().__init__(dialect, inst.__class__)
        self.inst = inst
        self.pk_query = self._pk_query()

    def _pk_query(self):
        query: QueryBuilder = (
            self.dialect.query_builder
            .from_(self.table.meta.pk_table)
            .delete()
        )
        for field in self.table.meta.fields.values():
            query = query.where(+field == getattr(self.inst, field.name))
        return query