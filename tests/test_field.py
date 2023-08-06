from datetime import datetime, timedelta
from typing import Any, Type, Union
from unittest import TestCase

from tydb.fields import BoolField, DateTimeField, Default, FloatField, IntField, Nullable, StrField
from tydb.models import Table
from tydb.session import AsyncSession, Session
from tydb.utils import maybe_await

try:
    from .utils import parametise, with_dialects
except ImportError:
    from tests.utils import parametise, with_dialects


NOW = datetime.now().astimezone()


class IntModel(Table, primary="field"):
    field = IntField()

class FloatModel(Table, primary="field"):
    field = FloatField()

class BoolModel(Table, primary="field"):
    field = BoolField()

class StrModel(Table, primary="field"):
    field = StrField(size=8)

class DateTimeModel(Table, primary="field"):
    field = DateTimeField()


Model = Union[IntModel, FloatModel, StrModel, BoolModel, DateTimeModel]


class _NullModelBase(Table, primary="id"):
    id = IntField(default=Default.SERVER)

class IntNullModel(_NullModelBase):
    field = Nullable.IntField()

class FloatNullModel(_NullModelBase):
    field = Nullable.FloatField()

class BoolNullModel(_NullModelBase):
    field = Nullable.BoolField()

class StrNullModel(_NullModelBase):
    field = Nullable.StrField()

class DateTimeNullModel(_NullModelBase):
    field = Nullable.DateTimeField()


NullModel = Union[IntNullModel, FloatNullModel, StrNullModel, BoolNullModel, DateTimeNullModel]


@with_dialects(
    IntModel, FloatModel, StrModel, BoolModel, DateTimeModel,
    IntNullModel, FloatNullModel, StrNullModel, BoolNullModel, DateTimeNullModel,
)
@parametise(
    (IntModel, 1, 2),
    (FloatModel, 1.5, 2.0),
    (BoolModel, True, False),
    (StrModel, "main", "alt"),
    (DateTimeModel, NOW - timedelta(1), NOW),
    (IntNullModel, None, 2),
    (FloatNullModel, None, 2.0),
    (BoolNullModel, None, False),
    (StrNullModel, None, "alt"),
    (DateTimeNullModel, None, NOW),
)
class TestField(TestCase):

    async def test_create(
        self, sess: Union[AsyncSession, Session], model: Type[Union[Model, NullModel]],
        value: Any, alt_value: Any,
    ):
        await maybe_await(sess.bulk_create([model.field], [value], [alt_value]))

    async def test_field_get(
        self, sess: Union[AsyncSession, Session], model: Type[Union[Model, NullModel]],
        value: Any, alt_value: Any,
    ):
        await maybe_await(sess.create(model, field=value))
        result = await maybe_await(sess.first(model, model.field == value))
        self.assertIsNotNone(result)
        self.assertEqual(result, model(id=1, field=value))
        result = await maybe_await(sess.first(model, model.field == alt_value))
        self.assertIsNone(result)
