from unittest import TestCase

from tydb.fields import IntField
from tydb.models import Table
from tydb.session import Session

from .utils import with_dialects


@with_dialects
class TestIntField(TestCase):

    class Model(Table, primary="field"):
        field = IntField()

    def test_field_create(self, sess: Session):
        inst = sess.create(self.Model, field=1)
        if inst is None:
            self.skipTest("Returning instance not supported by driver")
        self.assertEqual(inst, self.Model(field=1))

    def test_field_select(self, sess: Session):
        sess.create(self.Model, field=1)
        result = list(sess.select(self.Model))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], self.Model(field=1))

    def test_field_select_where(self, sess: Session):
        sess.create(self.Model, field=1)
        result = list(sess.select(self.Model, +self.Model.field == 1))
        self.assertEqual(len(result), 1)
        result = list(sess.select(self.Model, +self.Model.field == 2))
        self.assertEqual(len(result), 0)

    def test_field_remove(self, sess: Session):
        inst = sess.create(self.Model, field=1)
        if inst is None:
            self.skipTest("Returning instance not supported by driver")
        sess.remove(inst)
        result = list(sess.select(self.Model))
        self.assertEqual(len(result), 0)

    def test_field_delete(self, sess: Session):
        sess.create(self.Model, field=1)
        sess.delete(self.Model, 1)
        result = list(sess.select(self.Model))
        self.assertEqual(len(result), 0)
