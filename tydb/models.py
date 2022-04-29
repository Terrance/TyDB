from abc import ABC
from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union, overload
from typing_extensions import Self

import pypika


_TAny = TypeVar("_TAny")
_TTable = TypeVar("_TTable", bound="Table", covariant=True)

_RefSpec = Union["Reference[Table]", "Tuple[Reference[Table], ...]"]
_RefJoinSpec = Tuple[Tuple["Reference", ...], pypika.Table, pypika.Criterion]


def snake_case(value: str):
    return "".join("_" + char.lower() if char.isupper() else char for char in value).lstrip("_")


class Default:
    pass

DEFAULT = Default()


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

    def walk_refs(self, *seen: Type["Table"]) -> List[Tuple["Reference[Table]", ...]]:
        specs: List[Tuple[Reference[Table], ...]] = []
        for ref in self.references.values():
            if ref.table in seen:
                continue
            specs.append((ref,))
            for spec in ref.table.meta.walk_refs(ref.table, *seen):
                specs.append((ref,) + spec)
        return specs

    def join_refs(self, *specs: _RefSpec) -> List[_RefJoinSpec]:
        joins: List[_RefJoinSpec] = []
        aliases: Dict[Tuple["Reference[Table]", ...], pypika.Table] = {}
        for spec in specs:
            if not isinstance(spec, tuple):
                spec = (spec,)
            assert spec[0].owner is self.table
            for pos, ref in enumerate(spec):
                path = tuple(spec[:pos + 1])
                pk_parent = aliases[path[:-1]] if pos else self.pk_table
                pk_foreign = aliases.get(path)
                if not pk_foreign:
                    alias = "_{}_{}".format(len(aliases) + 1, ref.table.meta.name)
                    pk_foreign = ref.table.meta.pk_table.as_(alias)
                    aliases[path] = pk_foreign
                    join = pk_parent[ref.field.name] == pk_foreign[ref.field.foreign.name]
                    joins.append((path, pk_foreign, join))
        return joins


class _Descriptor(ABC, Generic[_TAny]):

    owner: Type[_TAny]
    name: str

    def __set_name__(self, owner: Type[_TAny], name: str):
        if hasattr(self, "owner"):
            raise RuntimeError("{} can't be assigned twice".format(self.__class__.__name__))
        self.owner = owner
        self.name = name


class Table(ABC):

    meta: TableMeta[Self]

    def __init_subclass__(cls: Type[Self]):
        cls.meta = TableMeta(cls)

    def __init__(self: Self, **data: Any):
        for name, field in self.meta.fields.items():
            self.__dict__[name] = field.decode(data[name])

    def __repr__(self):
        kwargs = ("{}={}".format(key, value) for key, value in self.__dict__.items())
        return "{}({})".format(self.__class__.__name__, ", ".join(kwargs))


class Field(_Descriptor[Table], ABC, Generic[_TAny]):

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

    def __init__(self, field: Field[Any], table: Type[_TTable], backref: Optional[str] = None):
        self.field = field
        self.table = table
        if backref:
            coll = Collection(self)
            setattr(table, backref, coll)
            coll.__set_name__(table, backref)

    @overload
    def __get__(self, obj: Table, objtype: Any = ...) -> _TTable: ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Reference[_TTable]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        try:
            return obj.__dict__[self.name]
        except KeyError:
            return BoundReference(self, obj)

    def __repr__(self):
        return "<{}: {}.{} ({})>".format(
            self.__class__.__name__, self.owner.__name__, self.name, self.table.__name__,
        )


class BoundReference(Generic[_TTable]):

    def __init__(self, ref: Reference[Table], inst: Table):
        self.ref = ref
        self.inst = inst

    def __repr__(self):
        return "<{}: {}.{} ({}) on {!r}>".format(
            self.__class__.__name__, self.ref.owner.__name__, self.ref.name,
            self.ref.owner.__name__, self.inst,
        )


class Collection(Generic[_TTable], _Descriptor[Table]):

    def __init__(self, ref: Reference):
        self.ref = ref

    @overload
    def __get__(self, obj: Table, objtype: Any = ...) -> "BoundCollection[_TTable]": ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Collection[_TTable]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        return BoundCollection(self, obj)

    def __repr__(self):
        return "<{}: {}.{} ({})>".format(
            self.__class__.__name__, self.owner.__name__, self.name, self.ref.owner.__name__,
        )


class BoundCollection(Generic[_TTable]):

    def __init__(self, coll: Collection, inst: Table):
        self.coll = coll
        self.inst = inst

    def __repr__(self):
        return "<{}: {}.{} ({}) on {!r}>".format(
            self.__class__.__name__, self.coll.owner.__name__, self.coll.name,
            self.coll.ref.owner.__name__, self.inst,
        )
