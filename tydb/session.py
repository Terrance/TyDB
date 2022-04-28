from typing import Any, Generator, Generic, Optional, Tuple, Type, TypeVar

import pypika
from pypika.queries import QueryBuilder

from .api import Connection, Cursor
from .models import _RefSpec, Table


_TTable = TypeVar("_TTable", bound=Table)
_TTableAlt = TypeVar("_TTableAlt", bound=Table)


class Query:

    pk_query: QueryBuilder

    def __init__(self, cursor: Cursor):
        self.cursor = cursor

    def execute(self) -> Generator[Tuple[Any, ...], None, None]:
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
        for result in super().execute():
            final, pos = self._unpack(self.table, result)
            for path, _, _ in self.joins:
                table = path[-1].table
                instance, offset = self._unpack(table, result[pos:])
                pos += offset
                target = final
                for ref in path[:-1]:
                    target = getattr(target, ref.field.name)
                setattr(target, path[-1].field.name, instance)
            yield final


class SelectOneQuery(SelectQuery[_TTable]):

    def execute(self) -> Optional[_TTable]:
        return next(super().execute(), None)


class Session:

    def __init__(self, conn: Connection):
        self.conn = conn

    def select(
        self, table: Type[_TTable], where: Optional[pypika.Criterion] = None, *joins: _RefSpec,
        auto_join: bool = False
    ) -> Generator[_TTable, None, None]:
        cur = self.conn.cursor()
        if auto_join:
            joins = tuple(table.meta.walk_refs())
        query = SelectQuery(cur, table, where, *joins)
        return query.execute()

    def get(
        self, table: Type[_TTable], where: Optional[pypika.Criterion] = None, *joins: _RefSpec,
        auto_join: bool = False
    ) -> Optional[_TTable]:
        cur = self.conn.cursor()
        if auto_join:
            joins = tuple(table.meta.walk_refs())
        query = SelectOneQuery(cur, table, where, *joins)
        return query.execute()
