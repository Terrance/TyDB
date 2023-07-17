from datetime import datetime, timedelta, timezone
from typing import Union
from unittest import TestCase
from tests.utils import parametise

from tydb.fields import DateTimeField, Default, IntField
from tydb.models import Table
from tydb.session import AsyncSession, Session
from tydb.utils import maybe_await

try:
    from .utils import with_dialects
except ImportError:
    from tests.utils import with_dialects


class Model(Table, primary="key"):
    key = IntField(default=Default.SERVER)
    date = DateTimeField()


DATE_NAIVE = datetime(2001, 2, 3, 4, 5, 6, 123456)
DATE_LOCAL = DATE_NAIVE.astimezone()
DATE_UTC = DATE_NAIVE.astimezone(timezone.utc)
DATE_REMOTE = DATE_NAIVE.astimezone(timezone(timedelta(hours=6)))


@with_dialects(Model)
@parametise((DATE_NAIVE,), (DATE_LOCAL,), (DATE_UTC,), (DATE_REMOTE,))
class TestFieldDate(TestCase):

    async def test_str(self, sess: Union[Session, AsyncSession], date: datetime):
        await maybe_await(sess.create(Model, date=date))
        model = await maybe_await(sess.get(Model))
        self.assertEqual(model.date, DATE_LOCAL)
