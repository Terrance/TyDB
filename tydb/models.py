from abc import ABC
from typing import (
    Any, Dict, Generic, Iterable, Iterator, List, Optional, Tuple, Type, TypeVar, Union, overload,
)
from typing_extensions import Self

import pypika
from pypika.queries import QueryBuilder

from .api import Cursor


_TAny = TypeVar("_TAny")
_TTable = TypeVar("_TTable", bound="Table")


def snake_case(value: str):
    return "".join("_" + char.lower() if char.isupper() else char for char in value).lstrip("_")


class Default:
    pass

DEFAULT = Default()


class Query:

    def __init__(self, query: QueryBuilder):
        self.pk_query = query

    def execute(self, cursor: Cursor) -> Iterable[Tuple[Any, ...]]:
        cursor.execute(str(self.pk_query))
        while True:
            result = cursor.fetchone()
            if not result:
                break
            yield result


class _TableQuery(Query, Generic[_TTable]):

    def __init__(self, table: Type[_TTable], query: QueryBuilder):
        super().__init__(query)
        self.table = table


class SelectQuery(_TableQuery[_TTable]):

    @classmethod
    def new(cls, table: Type[_TTable], where: Optional[pypika.Criterion] = None):
        pk_query: QueryBuilder = (
            pypika.Query
            .from_(table.meta.pk_table)
            .select(*table.meta.fields)
        )
        queue: List[Tuple[pypika.Table, Type["Table"]]] = [(table.meta.pk_table, table)]
        index = 0
        while queue:
            (parent, target), *queue = queue
            for ref in target.meta.references.values():
                index += 1
                alias = "_{}_{}".format(index, ref.table.meta.name)
                pk_foreign: pypika.Table = ref.table.meta.pk_table.as_(alias)
                cols = (pk_foreign[field] for field in ref.table.meta.fields)
                pk_query = (
                    pk_query
                    .join(pk_foreign)
                    .on(parent[ref.field.name] == pk_foreign[ref.field.foreign.name])
                    .select(*cols)
                )
                queue.append((pk_foreign, ref.table))
        if where:
            pk_query = pk_query.where(where)
        return cls(table, pk_query)

    def one(self) -> "SelectOneQuery[_TTable]":
        return SelectOneQuery(self.table, self.pk_query)

    def _unpack(self, table: _TTable, result: Tuple[Any, ...]) -> Tuple[_TTable, int]:
        fields = table.meta.fields
        size = len(fields)
        row = result[:size]
        data = dict(zip(fields, row))
        return (table(**data), size)

    def execute(self, cursor: Cursor) -> Iterator[_TTable]:
        for result in super().execute(cursor):
            queue: List[Tuple[Optional[Tuple[Table, str]], Type[Table]]] = [(None, self.table)]
            final, pos = self._unpack(self.table, result)
            while queue:
                (chain, table), *queue = queue
                if chain:
                    instance, offset = self._unpack(table, result[pos:])
                    pos += offset
                    parent, attr = chain
                    setattr(parent, attr, instance)
                else:
                    instance = final
                for ref in table.meta.references.values():
                    queue.append(((instance, ref.name), ref.table))
            yield final


class SelectOneQuery(SelectQuery[_TTable]):

    def execute(self, cursor: Cursor) -> Optional[_TTable]:
        return next(super().execute(cursor), None)


class TableMeta(Generic[_TTable]):

    def __init__(self: Self, table: Type[_TTable]):
        self.table = table
        self.name = snake_case(table.__name__)
        self.pk_table = pypika.Table(self.name)

    def _filter(self, cls: Type[_TAny]) -> Dict[str, _TAny]:
        return {name: value for name, value in vars(self.table).items() if isinstance(value, cls)}

    @property
    def fields(self) -> Dict[str, "Field[Any]"]:
        return self._filter(Field)

    @property
    def references(self) -> Dict[str, "Reference[Table]"]:
        return self._filter(Reference)

    def select(self, where: Optional[pypika.Criterion] = None) -> SelectQuery[_TTable]:
        return SelectQuery.new(self.table, where)


class _Descriptor(ABC, Generic[_TAny]):

    owner: Type[_TAny]
    name: str

    def __set_name__(self, owner: Type[_TAny], name: str):
        if hasattr(self, "owner"):
            raise RuntimeError("{} can't be assigned twice".format(self.__class__.__name__))
        self.owner = owner
        self.name = name


class Table:

    meta: TableMeta[Self]

    def __init_subclass__(cls: Type[Self]):
        cls.meta = TableMeta(cls)

    def __init__(self: Self, **data: Any):
        for name, field in self.meta.fields.items():
            self.__dict__[name] = field.decode(data[name])

    def __repr__(self):
        kwargs = ("{}={}".format(key, value) for key, value in self.__dict__.items())
        return "{}({})".format(self.__class__.__name__, ", ".join(kwargs))


class Field(_Descriptor[Table], Generic[_TAny]):

    data_type: Type[_TAny]

    def __init__(self, default: Union[_TAny, Default] = DEFAULT, foreign: Optional["Field"] = None):
        self.default: Any = default
        self.foreign = foreign

    @property
    def _pk_field(self) -> pypika.Field:
        return getattr(self.owner.meta.pk_table, self.name)

    @overload
    def __get__(self, obj: Table, objtype: Any = ...) -> _TAny: ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Field[_TAny]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        return obj.__dict__[self.name]

    def __pos__(self) -> pypika.Criterion:
        return self._pk_field

    def __repr__(self):
        return "<{}: {}.{}>".format(self.__class__.__name__, self.owner.__name__, self.name)

    def decode(self, value: Any) -> _TAny:
        return self.data_type(value)

    def encode(self, value: _TAny) -> Any:
        if self.data_type in (int, float, bool):
            return self.data_type(value)
        else:
            return str(value)


class Reference(_Descriptor[Table], Generic[_TTable]):

    def __init__(self, field: Field[Any], table: Type[_TTable]):
        self.field = field
        self.table = table

    @overload
    def __get__(self, obj: Table, objtype: Any = ...) -> _TTable: ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Reference[_TTable]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        return obj.__dict__[self.name]

    def __repr__(self):
        return "<{}: {}.{} ({})>".format(
            self.__class__.__name__, self.owner.__name__, self.name, self.table.__name__,
        )
