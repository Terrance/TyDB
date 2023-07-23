"""
Infrastructure for describing database tables and columns as Python classes.
"""

from copy import copy
from datetime import datetime
from enum import Enum, auto
import operator
from typing import Any, Dict, Generic, List, Optional, Sequence, Tuple, Type, TypeVar, Union, overload

import pypika
import pypika.terms
from typing_extensions import Self


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

    def __init__(self, table: Type[_TTable], name: Optional[str] = None, primary: Optional[str] = None):
        self.table = table
        self.name = name or snake_case(table.__name__)
        self.pk_table = pypika.Table(self.name)
        self.primary = None
        for cls in table.__bases__:
            if cls is Table:
                break
            elif issubclass(cls, Table):
                for name, field in cls.meta.fields.items():
                    field = copy(field)
                    field.owner = table
                    setattr(table, name, field)
                if primary is None and cls.meta.primary:
                    primary = cls.meta.primary.name
        if primary:
            self.primary = self.fields[primary]

    def _filter(self, cls: Type[_TAny]) -> Dict[str, _TAny]:
        matches: Dict[str, _TAny] = {}
        for name in dir(self.table):
            try:
                value = getattr(self.table, name)
            except AttributeError:
                continue
            if isinstance(value, cls):
                matches[name] = value
        return matches

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

    def __init_subclass__(cls, *, name: Optional[str] = None, primary: Optional[str] = None):
        cls.meta = TableMeta(cls, name, primary)

    def __init__(self, **data: Any):
        for name, field in self.meta.fields.items():
            try:
                value = data[name]
            except KeyError:
                if field.default in (Default.NONE, Default.SERVER):
                    raise
                elif field.default is Default.TIMESTAMP_NOW:
                    value = datetime.now().astimezone()
                else:
                    value = field.default
            self.__dict__[name] = field.decode(value)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.meta.primary:
            attr = self.meta.primary.name
            return getattr(self, attr) == getattr(other, attr)
        else:
            return self.__dict__ == other.__dict__

    def __repr__(self):
        kwargs: List[str] = []
        for name, field in self.meta.fields.items():
            value = getattr(self, name)
            if field.default is Default.NONE and value is None:
                continue
            elif value == field.default:
                continue
            kwargs.append("{}={!r}".format(name, value))
        return "{}({})".format(self.__class__.__name__, ", ".join(kwargs))


class _Term:

    def __eq__(self, other: Any):
        """
        Make a `this = other` clause (or `this IS NULL` if comparing with `None`).
        """
        if other is None:
            return Expr(pypika.terms.Term.isnull, self)
        else:
            return Expr(operator.eq, self, other)

    def __ne__(self, other: Any):
        """
        Make a `this <> other` clause (or `this IS NOT NULL` if comparing with `None`).
        """
        if other is None:
            return Expr(pypika.terms.Term.isnotnull, self)
        else:
            return Expr(operator.ne, self, other)

    def __lt__(self, other: Any):
        """
        Make a `this < other` clause.
        """
        return Expr(operator.lt, self, other)

    def __le__(self, other: Any):
        """
        Make a `this <= other` clause.
        """
        return Expr(operator.le, self, other)

    def __gt__(self, other: Any):
        """
        Make a `this > other` clause.
        """
        return Expr(operator.gt, self, other)

    def __ge__(self, other: Any):
        """
        Make a `this >= other` clause.
        """
        return Expr(operator.ge, self, other)
    
    def __neg__(self):
        """
        Make a `-this` clause.
        """
        return Expr(operator.neg, self)

    def __matmul__(self, other: Any):
        """
        Make a `this IN other` clause.
        """
        return Expr(pypika.terms.Term.isin, self, other)

    def __mul__(self, other: Any):
        """
        Make a `this LIKE other` clause.
        """
        return Expr(pypika.terms.Term.like, self, other)

    def __pow__(self, other: Any):
        """
        Make a `this ILIKE other` clause.
        """
        return Expr(pypika.terms.Term.ilike, self, other)


class Field(_Descriptor[Table], Generic[_TAny], _Term):
    """
    Representation of a database column.
    """

    data_type: Optional[Type[_TAny]] = None
    """Class that represents the Python type of this field's values."""

    def __init__(
        self, default: Union[_TAny, Default] = Default.NONE,
        foreign: Optional["Field[Any]"] = None,
    ):
        self.default = default
        self.foreign = foreign

    @property
    def pk_field(self) -> pypika.Field:
        return self.owner.meta.pk_table.field(self.name)

    @overload
    def __get__(self, obj: Table, objtype: Any = ...) -> _TAny: ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Field[_TAny]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        return obj.__dict__[self.name]

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.id)

    def decode(self, value: Any) -> _TAny:
        """
        Convert a value from a DB-API type to the appropriate Python type.
        """
        if self.data_type:
            return self.data_type(value)
        else:
            return value

    def encode(self, value: Any) -> Any:
        """
        Convert a value from a Python type to the appropriate DB-API type.
        """
        if isinstance(value, pypika.terms.Node):
            return value
        elif self.data_type and issubclass(self.data_type, (int, float, bool)):
            return self.data_type(value)
        else:
            return str(value)


class Expr(_Term):
    """
    Query expression, usable as a `WHERE` clause in supported queries.

    Instances of this class should not be created directly; instead, use Python operators on
    fields of tables to generate expressions.
    """

    def __init__(self, op, *args):
        self.op = op
        fields = [arg for arg in args if isinstance(arg, Field)]
        if len(fields) == 1:
            field = fields[0]
            values = []
            for arg in args:
                if isinstance(arg, (Field, pypika.terms.Term, str)):
                    values.append(arg)
                elif isinstance(arg, Sequence):
                    values.append(tuple(field.encode(item) for item in arg))
                else:
                    values.append(field.encode(arg))
            self.args = values
        else:
            self.args = args
    
    @staticmethod
    def _encode(obj: Any):
        if isinstance(obj, Expr):
            return obj.pk_frag
        elif isinstance(obj, Field):
            return obj.pk_field
        else:
            return obj
    
    @property
    def pk_frag(self):
        args = (self._encode(arg) for arg in self.args)
        return self.op(*args)

    def __and__(self, other: Any):
        """
        Make a `this AND other` clause.
        """
        return Expr(operator.and_, self, other)

    def __or__(self, other: Any):
        """
        Make a `this OR other` clause.
        """
        return Expr(operator.or_, self, other)
    
    def __invert__(self):
        """
        Make a `NOT this` clause.
        """
        return Expr(operator.invert, self)

    def __repr__(self):
        return "<{}: {} ({})>".format(
            self.__class__.__name__, self.op.__name__, ", ".join(repr(arg) for arg in self.args),
        )


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
    def __get__(self, obj: Table, objtype: Any = ...) -> "BoundReference[_TTable]": ...
    @overload
    def __get__(self, obj: None, objtype: Any = ...) -> "Reference[_TTable]": ...

    def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
        if not obj:
            return self
        bind = BoundReference(self, obj)
        setattr(obj, self.name, bind)
        return bind

    def __repr__(self):
        return "<{}: {} ({})>".format(self.__class__.__name__, self.id, self.table.__name__)


class BoundReference(Generic[_TTable]):
    """
    Placeholder reference on a model instance where the related object wasn't fetched.
    """

    value: _TTable

    def __init__(self, ref: Reference[_TTable], inst: Table):
        self.ref = ref
        self.inst = inst

    def __repr__(self):
        return "<{}: {} ({}) on {!r}{}>".format(
            self.__class__.__name__, self.ref.id, self.ref.table.__name__, self.inst,
            ": {!r}".format(self.value) if hasattr(self, "value") else ", not fetched",
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
