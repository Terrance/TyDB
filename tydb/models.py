from abc import ABC
from typing import Any, Generic, Iterator, Optional, Tuple, Type, TypeVar, Union, overload
from typing_extensions import Self

import pypika
from pypika.queries import QueryBuilder

from .api import Cursor


_TTable = TypeVar("_TTable", bound="Table")
_TCls = TypeVar("_TCls", bound=Type)


def snake_case(value: str):
    return "".join("_" + char.lower() if char.isupper() else char for char in value).lstrip("_")


class Default:
    pass

DEFAULT = Default()


class Query(Generic[_TTable]):

    def __init__(self, table: Type[_TTable], query: QueryBuilder):
        self.table = table
        self.pk_query = query

    def execute(self, cursor: Cursor) -> Iterator[Tuple[Any, ...]]:
        cursor.execute(str(self.pk_query))
        while True:
            result = cursor.fetchone()
            if not result:
                break
            yield result


class SelectQuery(Query[_TTable]):

    @classmethod
    def new(cls, table: Type[_TTable], where: Optional[pypika.Criterion] = None):
        pk_query: QueryBuilder = pypika.Query.from_(table.meta.pk_table).select(*table.meta.fields)
        if where:
            pk_query = pk_query.where(where)
        return SelectQuery(table, pk_query)

    def one(self) -> "SelectOneQuery[_TTable]":
        return SelectOneQuery(self.table, self.pk_query)

    def execute(self, cursor: Cursor) -> Iterator[_TTable]:
        for result in super().execute(cursor):
            data = dict(zip(self.table.meta.fields, result))
            yield self.table(data)


class SelectOneQuery(SelectQuery[_TTable]):

    def execute(self, cursor: Cursor) -> Optional[_TTable]:
        return next(super().execute(cursor), None)


class TableMeta(Generic[_TTable]):

    def __init__(self: Self, table: Type[_TTable]):
        self.table = table
        self.name = snake_case(table.__name__)
        self.pk_table = pypika.Table(self.name)

    @property
    def fields(self):
        return {name: value for name, value in vars(self.table).items() if isinstance(value, Field)}

    def select(self, where: Optional[pypika.Criterion] = None) -> SelectQuery[_TTable]:
        return SelectQuery.new(self.table, where)


class Table:

    meta: TableMeta[Self]

    def __init_subclass__(cls: Type[Self]):
        cls.meta = TableMeta(cls)

    def __init__(self: Self, data):
        self.__dict__.update(data)


class Field(ABC, Generic[_TCls]):

    data_type: Type[_TCls]

    table: Table
    name: str

    def __init__(self, default: Union[_TCls, Default] = DEFAULT):
        self.default: Any = default

    @property
    def _pk_field(self) -> pypika.Field:
        return getattr(self.table.meta.pk_table, self.name)

    def __set_name__(self, table: Table, name: str):
        if hasattr(self, "table"):
            raise RuntimeError("Field can't be assigned twice")
        self.table = table
        self.name = name

    @overload
    def __get__(self, obj: Table, objtype: Any = ...) -> _TCls: ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Field[_TCls]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        return obj.__dict__[self.name]

    def __set__(self, obj: Table, value: _TCls):
        obj.__dict__[self.name] = value

    def __pos__(self) -> pypika.Criterion:
        return self._pk_field

    def decode(self, value: str) -> _TCls:
        return self.data_type(value)

    def encode(self, value: _TCls) -> str:
        return str(value)
