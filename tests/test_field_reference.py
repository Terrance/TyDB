from typing import Union
from unittest import TestCase

from tydb.fields import BoolField, Default, IntField, Nullable
from tydb.models import BoundReference, Collection, Reference, Table
from tydb.session import AsyncSession, Session
from tydb.utils import maybe_await

try:
    from .utils import with_dialects
except ImportError:
    from tests.utils import with_dialects


class Inner(Table, primary="key"):
    key = IntField(default=Default.SERVER)
    value = BoolField(default=False)
    outers: Collection["Outer"]
    null_outers: Collection["NullOuter"]

class Outer(Table, primary="key"):
    key = IntField(default=Default.SERVER)
    inner_key = IntField(foreign=Inner.key)
    inner = Reference(inner_key, Inner)

class NullOuter(Table, primary="key"):
    key = IntField(default=Default.SERVER)
    inner_key = Nullable.IntField(foreign=Inner.key)
    inner = Nullable.Reference(inner_key, Inner)

Inner.outers = Collection(Outer.inner)
Inner.null_outers = Collection(NullOuter.inner)


@with_dialects(Inner, Outer, NullOuter)
class TestFieldReference(TestCase):

    async def test_get_joined(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(Inner))
        inner = await maybe_await(sess.get(Inner))
        await maybe_await(sess.create(Outer, inner_key=inner.key))
        outer = await maybe_await(sess.get(Outer, None, auto_join=True))
        self.assertEqual(inner, outer.inner.value)
        self.assertEqual(inner, await maybe_await(sess.load(outer.inner)))

    async def test_get_reference(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(Inner))
        inner = await maybe_await(sess.get(Inner))
        await maybe_await(sess.create(Outer, inner_key=inner.key))
        outer = await maybe_await(sess.get(Outer, None))
        self.assertIsInstance(outer.inner, BoundReference)
        self.assertEqual(inner, await maybe_await(sess.load(outer.inner)))

    async def test_collection(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(Inner))
        inner = await maybe_await(sess.get(Inner))
        await maybe_await(sess.create(Outer, inner_key=inner.key))
        await maybe_await(sess.create(Outer, inner_key=inner.key))
        from_model = [outer async for outer in await maybe_await(sess.select(Outer))]
        from_coll = [outer async for outer in await maybe_await(sess.select(inner.outers))]
        self.assertEqual(
            sorted(from_model, key=lambda outer: outer.key),
            sorted(from_coll, key=lambda outer: outer.key),
        )

    async def test_nullable_get_null(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(NullOuter))
        outer = await maybe_await(sess.get(NullOuter, None, auto_join=True))
        self.assertIsNone(outer.inner.value)
        self.assertIsNone(await maybe_await(sess.load(outer.inner)))

    async def test_nullable_get_joined(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(Inner))
        inner = await maybe_await(sess.get(Inner))
        await maybe_await(sess.create(NullOuter, inner_key=inner.key))
        outer = await maybe_await(sess.get(NullOuter, None, auto_join=True))
        self.assertEqual(inner, outer.inner.value)
        self.assertEqual(inner, await maybe_await(sess.load(outer.inner)))

    async def test_nullable_get_reference(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(Inner))
        inner = await maybe_await(sess.get(Inner))
        await maybe_await(sess.create(NullOuter, inner_key=inner.key))
        outer = await maybe_await(sess.get(NullOuter, None))
        self.assertIsInstance(outer.inner, BoundReference)
        self.assertEqual(inner, await maybe_await(sess.load(outer.inner)))

    async def test_nullable_collection(self, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.create(Inner))
        inner = await maybe_await(sess.get(Inner))
        await maybe_await(sess.create(NullOuter, inner_key=inner.key))
        await maybe_await(sess.create(NullOuter, inner_key=inner.key))
        from_model = [outer async for outer in await maybe_await(sess.select(NullOuter))]
        from_coll = [outer async for outer in await maybe_await(sess.select(inner.null_outers))]
        self.assertEqual(
            sorted(from_model, key=lambda outer: outer.key),
            sorted(from_coll, key=lambda outer: outer.key),
        )
