from itertools import count
import time
from typing import Union
from unittest import TestCase

from tydb.fields import IntField
from tydb.models import Table
from tydb.session import AsyncSession, Session
from tydb.utils import maybe_await

try:
    from .utils import with_dialects
except ImportError:
    from tests.utils import with_dialects


index = count(int(time.time() * 1000))


@with_dialects
class TestIntField(TestCase):

    class Model(Table, primary="field"):
        field = IntField()

    main_value = 1
    alt_value = 2

    async def test_field_create(self, sess: Union[AsyncSession, Session]):
        inst = await maybe_await(sess.create(self.Model, field=self.main_value))
        if inst is None:
            self.skipTest("Returning instance not supported by driver")
        self.assertEqual(inst, self.Model(field=self.main_value))

    async def test_field_select(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(self.Model, field=self.main_value))
        result = [inst async for inst in await maybe_await(sess.select(self.Model))]
        self.assertEqual(len(result), self.main_value)
        self.assertEqual(result[0], self.Model(field=self.main_value))

    async def test_field_select_where(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(self.Model, field=self.main_value))
        result = [inst async for inst in await maybe_await(sess.select(
            self.Model, +self.Model.field == self.main_value,
        ))]
        self.assertEqual(len(result), self.main_value)
        result = [inst async for inst in await maybe_await(sess.select(
            self.Model, +self.Model.field == self.alt_value,
        ))]
        self.assertEqual(len(result), 0)

    async def test_field_remove(self, sess: Union[AsyncSession, Session]):
        inst = await maybe_await(sess.create(self.Model, field=self.main_value))
        if inst is None:
            self.skipTest("Returning instance not supported by driver")
        await maybe_await(sess.remove(inst))
        result = [inst async for inst in await maybe_await(sess.select(self.Model))]
        self.assertEqual(len(result), 0)

    async def test_field_delete(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(self.Model, field=self.main_value))
        await maybe_await(sess.delete(self.Model, self.main_value))
        result = [inst async for inst in await maybe_await(sess.select(self.Model))]
        self.assertEqual(len(result), 0)
