from datetime import datetime
from typing import Union
from unittest import TestCase

from tydb.fields import Default, IntField, Nullable
from tydb.models import Table
from tydb.session import AsyncSession, Session
from tydb.utils import maybe_await

try:
    from .utils import with_dialects
except ImportError:
    from tests.utils import with_dialects


NOW = datetime.now().astimezone()


class Model(Table, primary="id"):
    id = IntField(default=Default.SERVER)
    text = Nullable.StrField()


@with_dialects(Model)
class TestQueries(TestCase):

    async def test_create(self, sess: Union[AsyncSession, Session]):
        key = await maybe_await(sess.create(Model))
        if key is None:
            self.skipTest("Database driver doesn't return primary key")
        self.assertEqual(key, 1)

    async def test_bulk_create(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        result = await maybe_await(sess.select(Model))
        insts = [item async for item in result]
        self.assertEqual(len(insts), 2)
        self.assertEqual(insts[0], Model(id=1, text=None))
        self.assertEqual(insts[1], Model(id=2, text="Text"))

    async def test_select(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(Model))
        result = await maybe_await(sess.select(Model))
        insts = [item async for item in result]
        self.assertEqual(len(insts), 1)
        self.assertEqual(insts[0], Model(id=1, text=None))

    async def test_select_where(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        result = await maybe_await(sess.select(Model, Model.id == 1))
        insts = [item async for item in result]
        self.assertEqual(len(insts), 1)
        self.assertEqual(insts[0], Model(id=1, text=None))

    async def test_select_where_missing(self, sess: Union[AsyncSession, Session]):
        result = await maybe_await(sess.select(Model, Model.id == 1))
        insts = [item async for item in result]
        self.assertEqual(len(insts), 0)

    async def test_get(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(Model))
        inst = await maybe_await(sess.get(Model))
        self.assertEqual(inst, Model(id=1, text=None))

    async def test_get_multiple(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        with self.assertRaises(LookupError):
            await maybe_await(sess.get(Model))

    async def test_get_missing(self, sess: Union[AsyncSession, Session]):
        with self.assertRaises(LookupError):
            await maybe_await(sess.get(Model))

    async def test_get_where(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        inst = await maybe_await(sess.get(Model, Model.id == 2))
        self.assertEqual(inst, Model(id=2, text="Text"))

    async def test_get_where_missing(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(Model))
        with self.assertRaises(LookupError):
            await maybe_await(sess.get(Model, Model.id == 2))

    async def test_first(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        inst = await maybe_await(sess.first(Model))
        self.assertIsNotNone(inst)
        self.assertEqual(inst, Model(id=1, text=None))

    async def test_first_missing(self, sess: Union[AsyncSession, Session]):
        inst = await maybe_await(sess.first(Model))
        self.assertIsNone(inst)

    async def test_first_where(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        inst = await maybe_await(sess.first(Model, Model.id == 1))
        self.assertIsNotNone(inst)
        self.assertEqual(inst, Model(id=1, text=None))

    async def test_first_where_missing(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.create(Model))
        inst = await maybe_await(sess.first(Model, Model.id == 2))
        self.assertIsNone(inst)

    async def test_remove(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        inst = await maybe_await(sess.get(Model, Model.id == 1))
        await maybe_await(sess.remove(inst))
        result = await maybe_await(sess.select(Model))
        insts = [item async for item in result]
        self.assertEqual(len(insts), 1)
        self.assertEqual(insts[0], Model(id=2, text="Text"))

    async def test_delete(self, sess: Union[AsyncSession, Session]):
        await maybe_await(sess.bulk_create([Model.text], [None], ["Text"]))
        await maybe_await(sess.delete(Model, 1))
        result = await maybe_await(sess.select(Model))
        insts = [item async for item in result]
        self.assertEqual(len(insts), 1)
        self.assertEqual(insts[0], Model(id=2, text="Text"))
