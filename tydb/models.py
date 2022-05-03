"""
Infrastructure for describing database tables and columns as Python classes.
"""

from enum import Enum, auto
from typing import (
    Any, Callable, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union, overload,
)
from typing_extensions import Self

import pypika


_TAny = TypeVar("_TAny")
_TTable = TypeVar("_TTable", bound="Table", covariant=True)

_RefSpec = Union["Reference[Table]", "Tuple[Reference[Table], ...]"]
_RefJoinSpec = Tuple[Tuple["Reference[Table]", ...], pypika.Table, pypika.Criterion]


def snake_case(value: str) -> str:
    """
    Convert a CamelCase name into snake_case.
    """
    return "".join("_" + char.lower() if char.isupper() else char for char in value).lstrip("_")


class Default(Enum):
    """
    Special default values for database columns.
    """

    NONE = auto()
    """No default -- a value for this column must be provided each time."""
    SERVER = auto()
    """Rely on the server to produce a value."""
    TIMESTAMP_NOW = auto()
    """Use the current timestamp as provided by the database host."""


class TableMeta(Generic[_TTable]):
    """
    Metadata and helper methods for a `Table` class, accessible via `Table.meta`.
    """

    def __init__(self, table: Type[_TTable], primary: Optional[str] = None):
        self.table = table
        self.name = snake_case(table.__name__)
        self.pk_table = pypika.Table(self.name)
        self.primary = self.fields[primary] if primary else None

    def _filter(self, cls: Type[_TAny]) -> Dict[str, _TAny]:
        return {name: value for name, value in vars(self.table).items() if isinstance(value, cls)}

    @property
    def fields(self) -> Dict[str, "Field[Any]"]:
        """
        Mapping of attribute names to `Field` objects, for each declared field.
        """
        return self._filter(Field)

    @property
    def references(self) -> Dict[str, "Reference[Table]"]:
        """
        Mapping of attribute names to `Reference` objects, for each declared reference.
        """
        return self._filter(Reference)

    def walk_refs(self, *seen: Type["Table"]) -> List[Tuple["Reference[Table]", ...]]:
        """
        Recursively follow `Reference` declarations on a `Table`, avoiding any cycles.

        Produces a list of tuples of foreign `Field` paths.
        """
        specs: List[Tuple[Reference[Table], ...]] = []
        for ref in self.references.values():
            if ref.table in seen:
                continue
            specs.append((ref,))
            for spec in ref.table.meta.walk_refs(ref.table, *seen):
                specs.append((ref,) + spec)
        return specs

    def join_refs(self, *specs: _RefSpec) -> List[_RefJoinSpec]:
        """
        Generate the metadata needed to join the given `Reference` declarations.

        Produces a list of tuples of (foreign `Field` path tuple, table alias, join condition).
        """
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
                    assert ref.field.foreign
                    join = pk_parent[ref.field.name] == pk_foreign[ref.field.foreign.name]
                    joins.append((path, pk_foreign, join))
        return joins


class _Descriptor(Generic[_TAny]):

    owner: Type[_TAny]
    name: str

    def __set_name__(self, owner: Type[_TAny], name: str):
        if hasattr(self, "owner"):
            raise RuntimeError("{} can't be assigned twice".format(self.__class__.__name__))
        self.owner = owner
        self.name = name

    @property
    def id(self) -> Optional[str]:
        """
        Owning class and attribute name representation, once the descriptor has been assigned.
        """
        if hasattr(self, "owner"):
            return "{}.{}".format(self.owner.__name__, self.name)
        else:
            return "<unbound>"


class Table:
    """
    Representation of a database table.
    """

    meta: TableMeta[Self]

    def __init_subclass__(cls, primary: Optional[str] = None):
        cls.meta = TableMeta(cls, primary)

    def __init__(self, **data: Any):
        for name, field in self.meta.fields.items():
            self.__dict__[name] = field.decode(data[name])

    def __repr__(self):
        kwargs = ("{}={!r}".format(key, value) for key, value in self.__dict__.items())
        return "{}({})".format(self.__class__.__name__, ", ".join(kwargs))


class Field(_Descriptor[Table], Generic[_TAny]):
    """
    Representation of a database column.
    """

    data_type: Union[Type[_TAny], Callable[[Any], _TAny]] = staticmethod(lambda x: x)
    """Class or callable that converts a value to the appropriate Python type."""

    def __init__(
        self, default: Union[_TAny, Default] = Default.NONE,
        foreign: Optional["Field[Any]"] = None,
    ):
        self.default = default
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
        return "<{}: {}>".format(self.__class__.__name__, self.id)

    def decode(self, value: Any) -> _TAny:
        """
        Convert a value from a DB-API type to the appropriate Python type.
        """
        return self.data_type(value)

    def encode(self, value: _TAny) -> Any:
        """
        Convert a value from a Python type to the appropriate DB-API type.
        """
        if self.data_type in (int, float, bool):
            return self.data_type(value)
        else:
            return str(value)


class Reference(_Descriptor[Table], Generic[_TTable]):
    """
    Representation of a foreign key.
    """

    def __init__(self, field: Field[Any], table: Type[_TTable], backref: Optional[str] = None):
        self.field = field
        if not field.foreign:
            raise ValueError("Reference field {} not foreign".format(field.id))
        if field.foreign.owner is not table:
            msg = "Reference table {} doesn't match field's related table {}"
            raise TypeError(msg.format(table.__name__, field.foreign.owner.__name__))
        self.table = table
        if backref:
            coll = Collection(self)
            setattr(table, backref, coll)
            coll.__set_name__(table, backref)

    def __set_name__(self, owner: Type[Table], name: str):
        super().__set_name__(owner, name)
        if owner is not self.field.owner:
            msg = "Reference field {} on foreign table {}"
            raise TypeError(msg.format(self.field.id, self.field.owner.__name__))

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
        return "<{}: {} ({})>".format(self.__class__.__name__, self.id, self.table.__name__)


class BoundReference(Generic[_TTable]):
    """
    Placeholder reference on a model instance where the related object wasn't fetched.
    """

    def __init__(self, ref: Reference[Table], inst: Table):
        self.ref = ref
        self.inst = inst

    def __repr__(self):
        return "<{}: {} ({}) on {!r}>".format(
            self.__class__.__name__, self.ref.id, self.ref.owner.__name__, self.inst,
        )


class Collection(Generic[_TTable], _Descriptor[Table]):
    """
    Representation of the reversed query of a foreign key.
    """

    def __init__(self, ref: Reference[Table]):
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
        return "<{}: {} ({})>".format(self.__class__.__name__, self.id, self.ref.owner.__name__)


class BoundCollection(Generic[_TTable]):
    """
    Placeholder collection on a model instance.
    """

    def __init__(self, coll: Collection[_TTable], inst: Table):
        self.coll = coll
        self.inst = inst

    def __repr__(self):
        return "<{}: {} ({}) on {!r}>".format(
            self.__class__.__name__, self.coll.id, self.coll.ref.owner.__name__, self.inst,
        )
