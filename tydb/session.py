"""
Sessions represent the high-level interface to interact with data in a database.
"""

from datetime import datetime
import logging
from typing import Any, Awaitable, List, Optional, Tuple, Type, TypeVar, Union

import pypika

from .api import AsyncConnection, Connection
from .dialects import Dialect
from .models import _RefSpec, BoundCollection, BoundReference, Default, Field, Table
from .queries import (
    AsyncInsertQuery, AsyncSelectQuery, AsyncSelectOneQuery, AsyncSelectQueryResult,
    CreateTableQuery, DeleteQuery, DeleteOneQuery, DropTableQuery, InsertQuery,
    SelectOneQuery, SelectQuery, SelectQueryResult, _SelectQueryResult,
)
from .utils import maybe_await


_T = TypeVar("_T")
_TTable = TypeVar("_TTable", bound=Table)
_MaybeAsync = Union[_T, Awaitable[_T]]


LOG = logging.getLogger(__name__)


class _Session:

    def __init__(self, conn: Union[Connection, AsyncConnection], dialect: Type[Dialect] = Dialect):
        self.conn = conn
        self.dialect = dialect

    def __repr__(self):
        return "<{}: {!r} {}>".format(self.__class__.__name__, self.conn, self.dialect.__name__)

    def setup(self, *tables: Type[Table]) -> None:
        """
        Perform `CREATE TABLE` queries for the given tables.
        """
        raise NotImplementedError

    def destroy(self, *tables: Type[Table]) -> None:
        """
        Perform `DROP TABLE` queries for the given tables.
        """
        raise NotImplementedError

    def _select_joins(self, table: Type[_TTable], *joins: _RefSpec, auto_join: bool = False):
        return tuple(table.meta.walk_refs()) if auto_join else joins

    def _select_ref(
        self, table: Union[Type[_TTable], BoundCollection[_TTable]], where: Optional[pypika.Criterion] = None,
        *joins: _RefSpec, auto_join: bool = False,
    ) -> Tuple[Type[_TTable], Optional[pypika.Criterion], Tuple[_RefSpec]]:
        if isinstance(table, BoundCollection):
            bind = table
            table = bind.coll.ref.owner
            assert bind.coll.ref.field.foreign
            relate = +bind.coll.ref.field == getattr(bind.inst, bind.coll.ref.field.foreign.name)
            where = relate & where if where else relate
        joins = self._select_joins(table, *joins, auto_join=auto_join)
        return (table, where, joins)

    def select(
        self, table: Union[Type[_TTable], BoundCollection[_TTable]],
        where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> _SelectQueryResult[_TTable]:
        """
        Perform a `SELECT` query against the given table or collection.
        """
        raise NotImplementedError

    def get(
        self, table: Type[_TTable], where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        """
        Perform a `SELECT ... LIMIT 1` query against the given table.

        Returns the first matching record, or `None` if there are no matches.
        """
        raise NotImplementedError

    def _load_where(
        self, bind: BoundReference[_TTable], *joins: _RefSpec, auto_join: bool = False,
    ) -> Tuple[Optional[pypika.Criterion], Tuple[_RefSpec]]:
        assert bind.ref.field.foreign
        where = +bind.ref.field.foreign == getattr(bind.inst, bind.ref.field.name)
        joins = self._select_joins(bind.ref.table, *joins, auto_join=auto_join)
        return (where, joins)

    def load(
        self, bind: Union[_TTable, BoundReference[_TTable]], *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        """
        Perform a `SELECT ... LIMIT 1` query for a referenced object.
        """
        raise NotImplementedError

    def _create_fields(self, table: Type[_TTable], **data: Any) -> Tuple[List[Any], List[Field[Any]]]:
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
        return row, fields

    def create(self, table: Type[_TTable], **data: Any) -> Optional[_TTable]:
        """
        Perform an `INSERT` query to add a new record to the given table.

        Returns the new instance if the underlying connection provides the last row ID.
        """
        raise NotImplementedError

    def _remove_query(self, inst: Table, *insts: Table) -> Union[DeleteOneQuery, DeleteQuery]:
        table = inst.__class__
        if table.meta.primary:
            return DeleteQuery(self.dialect, table, inst, *insts)
        elif not insts:
            return DeleteOneQuery(self.dialect, inst)
        else:
            raise RuntimeError("Can only delete single instance of table without primary key")

    def remove(self, *insts: Table) -> _MaybeAsync[None]:
        """
        Perform a `DELETE` query that removes the given instances.
        """
        raise NotImplementedError

    def delete(self, table: Type[Table], *ids: Any) -> _MaybeAsync[None]:
        """
        Perform a `DELETE` query that removes rows of the given table by primary key value.
        """
        raise NotImplementedError


class Session(_Session):
    """
    Wrapper around a DB-API `Connection`.
    """

    conn: Connection

    def setup(self, *tables: Type[Table]) -> None:
        queries = (CreateTableQuery(self.dialect, table) for table in tables)
        cursor = self.conn.cursor()
        for query in queries:
            query.execute(cursor)

    def destroy(self, *tables: Type[Table]) -> None:
        queries = (DropTableQuery(self.dialect, table) for table in tables)
        cursor = self.conn.cursor()
        for query in queries:
            query.execute(cursor)

    def select(
        self, table: Union[Type[_TTable], BoundCollection[_TTable]],
        where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> SelectQueryResult[_TTable]:
        table, where, joins = self._select_ref(table, where, *joins, auto_join=auto_join)
        query = SelectQuery(self.dialect, table, where, *joins)
        cursor = self.conn.cursor()
        return query.execute(cursor)

    def get(
        self, table: Type[_TTable], where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        joins = self._select_joins(table, *joins, auto_join=auto_join)
        query = SelectOneQuery(self.dialect, table, where, *joins)
        cursor = self.conn.cursor()
        return query.execute(cursor)

    def load(
        self, bind: Union[_TTable, BoundReference[_TTable]], *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        if isinstance(bind, Table):
            return bind
        where, joins = self._load_where(bind, *joins, auto_join=auto_join)
        query = SelectOneQuery(self.dialect, cast(Type[_TTable], bind.ref.table), where, *joins)
        cursor = self.conn.cursor()
        return query.execute(cursor)

    def create(self, table: Type[_TTable], **data: Any) -> Optional[_TTable]:
        row, fields = self._create_fields(table, **data)
        query = InsertQuery(self.dialect, table, row, fields=fields)
        cursor = self.conn.cursor()
        primary = query.execute(cursor)
        if primary is not None and table.meta.primary:
            return self.get(table, +table.meta.primary == primary)

    def remove(self, *insts: Table) -> None:
        if not insts:
            return
        query = self._remove_query(*insts)
        cursor = self.conn.cursor()
        query.execute(cursor)

    def delete(self, table: Type[Table], *ids: Any) -> None:
        if not ids:
            return
        query = DeleteQuery(self.dialect, table, *ids)
        cursor = self.conn.cursor()
        query.execute(cursor)


class AsyncSession(_Session):
    """
    Wrapper around an asynchronous DB-API-like `Connection`.

    Each call to a connection method will be `await`ed if it returns an awaitable object.
    """

    conn: AsyncConnection

    async def setup(self, *tables: Type[Table]) -> None:
        queries = (CreateTableQuery(self.dialect, table) for table in tables)
        cursor = await maybe_await(self.conn.cursor())
        for query in queries:
            await query.execute(cursor)

    async def destroy(self, *tables: Type[Table]) -> None:
        queries = (DropTableQuery(self.dialect, table) for table in tables)
        cursor = await maybe_await(self.conn.cursor())
        for query in queries:
            await query.execute(cursor)

    async def select(
        self, table: Union[Type[_TTable], BoundCollection[_TTable]],
        where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> AsyncSelectQueryResult[_TTable]:
        table, where, joins = self._select_ref(table, where, *joins, auto_join=auto_join)
        query = AsyncSelectQuery(self.dialect, table, where, *joins)
        cursor = await maybe_await(self.conn.cursor())
        return await query.execute(cursor)

    async def get(
        self, table: Type[_TTable], where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        joins = self._select_joins(table, *joins, auto_join=auto_join)
        query = AsyncSelectOneQuery(self.dialect, table, where, *joins)
        cursor = await maybe_await(self.conn.cursor())
        return await query.execute(cursor)

    async def load(
        self, bind: Union[_TTable, BoundReference[_TTable]], *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        if isinstance(bind, Table):
            return bind
        where, joins = self._load_where(bind, *joins, auto_join=auto_join)
        query = AsyncSelectOneQuery(self.dialect, cast(Type[_TTable], bind.ref.table), where, *joins)
        cursor = await maybe_await(self.conn.cursor())
        return await query.execute(cursor)

    async def create(self, table: Type[_TTable], **data: Any) -> Optional[_TTable]:
        row, fields = self._create_fields(table, **data)
        query = AsyncInsertQuery(self.dialect, table, row, fields=fields)
        cursor = await maybe_await(self.conn.cursor())
        primary = await query.execute(cursor)
        if (
            table.meta.primary and table.meta.primary.data_type and
            isinstance(primary, table.meta.primary.data_type)
        ):
            return await self.get(table, +table.meta.primary == primary)

    async def remove(self, *insts: Table) -> None:
        if not insts:
            return
        query = self._remove_query(*insts)
        cursor = await maybe_await(self.conn.cursor())
        await query.execute(cursor)

    async def delete(self, table: Type[Table], *ids: Any) -> None:
        if not ids:
            return
        query = DeleteQuery(self.dialect, table, *ids)
        cursor = await maybe_await(self.conn.cursor())
        await query.execute(cursor)
