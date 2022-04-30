import logging
from typing import Any, Generator, Generic, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import pypika
from pypika.queries import QueryBuilder

from .api import Connection, Cursor
from .models import _RefSpec, BoundCollection, BoundReference, Default, Field, Table


_TTable = TypeVar("_TTable", bound=Table)
_TTableAlt = TypeVar("_TTableAlt", bound=Table)


LOG = logging.getLogger(__name__)


class Query:
    """
    Representation of an SQL query.
    """

    pk_query: QueryBuilder

    def __init__(self, cursor: Cursor):
        self.cursor = cursor

    def execute(self) -> Generator[Tuple[Any, ...], None, None]:
        """
        Perform the query against the database associated with the provided cursor.

        Yields tuples of rows returned by the query, if any.  As this method produces a generator,
        its return value must either by iterated over or exausted in order to run the query.
        """
        LOG.debug(self.pk_query)
        self.cursor.execute(str(self.pk_query))
        while True:
            result = self.cursor.fetchone()
            if not result:
                break
            yield result


class _TableQuery(Query, Generic[_TTable]):

    def __init__(self, cursor: Cursor, table: Type[_TTable]):
        super().__init__(cursor)
        self.table = table


class SelectQuery(_TableQuery[_TTable]):
    """
    Representation of a `SELECT` SQL query.
    """

    def __init__(
        self, cursor: Cursor, table: Type[_TTable], where: Optional[pypika.Criterion] = None,
        *refs: _RefSpec,
    ):
        super().__init__(cursor, table)
        self.where = where
        self.joins = table.meta.join_refs(*refs)
        self.pk_query = self._pk_query()

    def _pk_query(self):
        query: QueryBuilder = (
            pypika.Query
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

    def execute(self) -> Generator[_TTable, None, None]:
        """
        Like `Query.execute`, but yields the results as instances of the table's class.
        """
        for result in super().execute():
            final, pos = self._unpack(self.table, result)
            for path, _, _ in self.joins:
                table = path[-1].table
                instance, offset = self._unpack(table, result[pos:])
                pos += offset
                target = final
                for ref in path[:-1]:
                    target = getattr(target, ref.field.name)
                setattr(target, path[-1].name, instance)
            yield final


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
        self, cursor: Cursor, table: Type[_TTable], *rows: Iterable[Any],
        fields: Optional[Iterable["Field"]] = None,
    ):
        super().__init__(cursor, table)
        self.rows = rows
        self.fields = fields or table.meta.fields.values()
        self.pk_query = self._pk_query()

    def _pk_query(self):
        cols = (field.name for field in self.fields)
        return (
            pypika.Query
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
        list(super().execute())
        return self.cursor.lastrowid if self.cursor.lastrowid not in (None, -1) else None


class Session:
    """
    Wrapper around a DB-API-compatible `Connection`.
    """

    def __init__(self, conn: Connection):
        self.conn = conn

    def select(
        self, table: Union[Type[_TTable], BoundCollection[_TTable]],
        where: Optional[pypika.Criterion] = None, *joins: _RefSpec, auto_join: bool = False,
    ) -> Generator[_TTable, None, None]:
        """
        Perform a `SELECT` query against the given table or collection.
        """
        if isinstance(table, BoundCollection):
            bind, table = table, table.coll.ref.owner
            relate = +bind.coll.ref.field == getattr(bind.inst, bind.coll.ref.field.foreign.name)
            where = relate & where if where else relate
        if auto_join:
            joins = tuple(table.meta.walk_refs())
        cur = self.conn.cursor()
        query = SelectQuery(cur, table, where, *joins)
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
        query = SelectOneQuery(cur, table, where, *joins)
        return query.execute()

    def load(
        self, bind: BoundReference[_TTable], *joins: _RefSpec, auto_join: bool = False,
    ) -> Optional[_TTable]:
        """
        Perform a `SELECT ... LIMIT 1` query for a referenced object.
        """
        where = +bind.ref.field.foreign == getattr(bind.inst, bind.ref.field.name)
        if auto_join:
            joins = tuple(bind.ref.table.meta.walk_refs())
        cur = self.conn.cursor()
        query = SelectOneQuery(cur, bind.ref.table, where, *joins)
        return query.execute()

    def create(self, table: Type[_TTable], **data: Any) -> Optional[_TTable]:
        """
        Perform an `INSERT` query to add a new record to the given table.

        Returns the new instance if the underlying connection provides the last row ID.
        """
        fields: List[Field] = []
        row = []
        for name, field in table.meta.fields.items():
            try:
                value = data[name]
            except KeyError:
                if field.default is Default.NONE:
                    raise
                value = field.default
            row.append(value)
            fields.append(field)
        cur = self.conn.cursor()
        query = InsertQuery(cur, table, row, fields=fields)
        primary = query.execute()
        if primary is not None and table.meta.primary:
            return self.get(table, +table.meta.primary == primary)
