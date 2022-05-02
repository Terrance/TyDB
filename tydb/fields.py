from datetime import datetime
from typing import Any, Generic, Optional, Type, TypeVar

from .models import Default, Field


_TAny = TypeVar("_TAny")


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
    """
    data_type = str


class DateTimeField(Field[datetime]):
    """
    Representation of a timestamp database column.
    """
    data_type = datetime

    def __init__(self, default_now: bool = False, **kwargs: Any):
        super().__init__(**kwargs)
        if default_now:
            self.default = Default.TIMESTAMP_NOW

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
        return super().encode(value)


class Nullable:
    """
    Derivatives of fields that also accept null as a database value.
    """

    class _NullableField(Field[Optional[_TAny]], Generic[_TAny]):

        data_type: Type[_TAny]

        def decode(self, value: Optional[Any]) -> Optional[_TAny]:
            return super().decode(value) if value is not None else None

        def encode(self, value: _TAny) -> Optional[Any]:
            return super().encode(value) if value is not None else None

    @classmethod
    def is_nullable(cls, field: Type[Field[Any]]) -> bool:
        return issubclass(field, cls._NullableField)

    @classmethod
    def non_null_type(cls, field: Type[Field[Any]]) -> Type[Field[Any]]:
        if cls.is_nullable(field):
            base = field.__bases__[1]
            assert issubclass(base, Field)
            return base
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
