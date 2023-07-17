"""
Fields handle the conversions between native types like `int` or `str` and their encoded forms
needed in SQL queries.

By default, fields do not accept `None` as a value, and will be declared in the database as `NOT
NULL` -- the `Nullable` set of fields explicitly removes this restriction.
"""

from datetime import datetime
from typing import Any, Optional, Type, TypeVar, Union, overload

from .models import BoundReference, Default, Field, Reference, Table


_TAny = TypeVar("_TAny")
_TTable = TypeVar("_TTable", bound=Table)


class IntField(Field[int]):
    """
    Representation of an integer database column.
    """
    data_type = int


class FloatField(Field[float]):
    """
    Representation of an decimal database column.
    """
    data_type = float


class BoolField(Field[bool]):
    """
    Representation of a boolean database column.
    """
    data_type = bool


class StrField(Field[str]):
    """
    Representation of a string or varchar database column.

    Some dialects may require the `size` parameter if using this field as a primary key.
    """
    data_type = str

    def __init__(
        self, default: Union[str, Default] = Default.NONE,
        foreign: Optional["Field[Any]"] = None, size: Optional[int] = None,
    ):
        super().__init__(default, foreign)
        self.size = size


class DateTimeField(Field[datetime]):
    """
    Representation of a timestamp database column.

    Timestamps are expected to be timezone-aware -- naive `datetime` instances will be treated as
    local time and given the corresponding timezone.
    """
    data_type = datetime

    def decode(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone()
        elif isinstance(value, str):
            return datetime.fromisoformat(value).astimezone()
        else:
            raise TypeError(value)

    def encode(self, value: datetime) -> Any:
        if not value.tzinfo:
            value = value.astimezone()
        return super().encode(value.isoformat())


class Nullable:
    """
    Derivatives of fields that also accept null as a database value.
    """

    class _Nullable:
        pass

    class _NullableField(_Nullable, Field[Optional[_TAny]]):

        data_type: Type[_TAny]

        def decode(self, value: Optional[Any]) -> Optional[_TAny]:
            return super().decode(value) if value is not None else None

        def encode(self, value: _TAny) -> Optional[Any]:
            return super().encode(value) if value is not None else None

    @classmethod
    def is_nullable(cls, field: Type[Union[Field[Any], Reference[Any]]]) -> bool:
        """
        Test if a field type is nullable.
        """
        return issubclass(field, cls._Nullable)

    @overload
    def non_null_type(cls, field: Type[Field[Any]]) -> Type[Field[Any]]: ...
    @overload
    def non_null_type(cls, field: Type[Reference[Any]]) -> Type[Reference[Any]]: ...

    @classmethod
    def non_null_type(cls, field: Type[Union[Field[Any], Reference[Any]]]):
        """
        Derive the non-nullable base field type from a nullable subclass.
        """
        if cls.is_nullable(field):
            return next((base for base in field.__bases__ if not issubclass(base, cls._Nullable)))
        else:
            return field

    class IntField(_NullableField[int], IntField):
        pass

    class FloatField(_NullableField[float], FloatField):
        pass

    class BoolField(_NullableField[bool], BoolField):
        pass

    class StrField(_NullableField[str], StrField):
        pass

    class DateTimeField(_NullableField[str], DateTimeField):
        pass

    class Reference(_Nullable, Reference[_TTable]):

        @overload
        def __get__(self, obj: Table, objtype: Any = ...) -> "Nullable.BoundReference[_TTable]": ...
        @overload
        def __get__(self, obj: None, objtype: Any = ...) -> "Nullable.Reference[_TTable]": ...

        def __get__(self, obj: Optional[Table], objtype: Optional[Type[Table]] = None):
            return super().__get__(obj, objtype)

    class BoundReference(_Nullable, BoundReference[_TTable]):
        
        value: Optional[_TTable]
