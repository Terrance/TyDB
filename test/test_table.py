from unittest import TestCase

import pypika as pk

from tydb.models import Field, Reference, Table


class TestTable(TestCase):

    def test_name(self):
        for name, canonical in (("lower", "lower"), ("Title", "title"), ("TwoWords", "two_words")):
            with self.subTest(name=name, canonical=canonical):  
                Model = type(name, (Table,), {})
                self.assertEqual(Model.meta.name, canonical)

    def test_field_primary(self):
        class Model(Table, primary="field"):
            field = Field()
        self.assertEqual(Model.meta.primary, Model.field)

    def test_field_primary_missing(self):
        with self.assertRaises(KeyError):
            class _(Table, primary="missing"):
                field = Field()

    def test_field_reassign(self):
        field = Field()
        with self.assertRaises(RuntimeError):
            class _(Table):
                field1 = field
                field2 = field

    def test_field_late(self):
        class Model(Table):
            field: Field
        Model.field = Field()
        self.assertEqual(Model.meta.fields, {"field": Model.field})

    def test_ref_invalid_foreign_unset(self):
        class Other(Table):
            key = Field()
        with self.assertRaises(ValueError) as ctx:
            class _(Table):
                other_key = Field()
                other = Reference(other_key, Other)
        self.assertEqual(ctx.exception.args[0], "Reference field <unbound> not foreign")

    def test_ref_invalid_table_mismatch(self):
        class Different(Table):
            key = Field()
        class Other(Table):
            key = Field()
        with self.assertRaises(TypeError) as ctx:
            class _(Table):
                other_key = Field(foreign=Other.key)
                other = Reference(other_key, Different)
        msg = "Reference table Different doesn't match field's related table Other"
        self.assertEqual(ctx.exception.args[0], msg)

    def test_ref_invalid_table_foreign(self):
        class Other(Table):
            key = Field()
            alt_key = Field(foreign=key)
        with self.assertRaises(RuntimeError) as ctx:
            class _(Table):
                other = Reference(Other.alt_key, Other)
        cause = ctx.exception.__cause__
        self.assertIsInstance(cause, TypeError)
        self.assertEqual(cause.args[0], "Reference field Other.alt_key on foreign table Other")

    def test_ref_valid(self):
        class Other(Table):
            key = Field()
        class Model(Table):
            other_key = Field(foreign=Other.key)
            other = Reference(other_key, Other)
        self.assertEqual(Model.meta.references, {"other": Model.other})

    def test_ref_self(self):
        class Loop(Table):
            key = Field()
            loop_key: Field
            loop: Reference["Loop"]
        Loop.loop_key = Field(foreign=Loop.key)
        Loop.loop = Reference(Loop.loop_key, Loop)
        self.assertEqual(Loop.meta.references, {"loop": Loop.loop})

    def test_ref_walk(self):
        class Inner(Table):
            key = Field()
        class Mid(Table):
            key = Field()
            inner_key = Field(foreign=Inner.key)
            inner = Reference(inner_key, Inner)
        class Outer(Table):
            mid_key = Field(foreign=Mid.key)
            mid = Reference(mid_key, Mid)
        with self.subTest("outer"):
            self.assertEqual(Outer.meta.walk_refs(), [(Outer.mid,), (Outer.mid, Mid.inner)])
        with self.subTest("mid"):
            self.assertEqual(Mid.meta.walk_refs(), [(Mid.inner,)])
        with self.subTest("inner"):
            self.assertEqual(Inner.meta.walk_refs(), [])

    def test_ref_walk_recursive(self):
        class Forth(Table):
            key = Field()
            back_key: Field
            back: Reference["Back"]
        class Back(Table):
            key = Field()
            forth_key = Field(foreign=Forth.key)
            forth = Reference(forth_key, Forth)
        Forth.back_key = Field(foreign=Back.key)
        Forth.back = Reference(Forth.back_key, Back)
        with self.subTest("forth"):
            self.assertEqual(Forth.meta.walk_refs(), [(Forth.back,), (Forth.back, Back.forth)])
        with self.subTest("back"):
            self.assertEqual(Back.meta.walk_refs(), [(Back.forth,), (Back.forth, Forth.back)])
        class Loop(Table):
            key = Field()
            loop_key: Field
            loop: Reference["Loop"]
        Loop.loop_key = Field(foreign=Loop.key)
        Loop.loop = Reference(Loop.loop_key, Loop)
        with self.subTest("loop"):
            self.assertEqual(Loop.meta.walk_refs(), [(Loop.loop,)])

    def test_ref_join(self):
        class Inner(Table):
            key = Field()
        class Mid(Table):
            key = Field()
            inner_1_key = Field(foreign=Inner.key)
            inner_1 = Reference(inner_1_key, Inner)
            inner_2_key = Field(foreign=Inner.key)
            inner_2 = Reference(inner_2_key, Inner)
            inner_3_key = Field(foreign=Inner.key)
            inner_3 = Reference(inner_3_key, Inner)
        class Outer(Table):
            mid_1_key = Field(foreign=Mid.key)
            mid_1 = Reference(mid_1_key, Mid)
            mid_2_key = Field(foreign=Mid.key)
            mid_2 = Reference(mid_2_key, Mid)
        specs = (Outer.mid_2, (Outer.mid_1, Mid.inner_1), (Outer.mid_1, Mid.inner_2))
        joins = Outer.meta.join_refs(*specs)
        with self.subTest("mid_2"):
            alias = pk.Table("mid").as_("_1_mid")
            join = pk.Table("outer").mid_2_key == alias.key
            self.assertEqual(joins[0], ((Outer.mid_2,), alias, join))
        with self.subTest("mid_1"):
            alias = pk.Table("mid").as_("_2_mid")
            join = pk.Table("outer").mid_1_key == alias.key
            self.assertEqual(joins[1], ((Outer.mid_1,), alias, join))
        with self.subTest("mid_1_inner_1"):
            inner_alias = pk.Table("inner").as_("_3_inner")
            join = alias.inner_1_key == inner_alias.key
            self.assertEqual(joins[2], ((Outer.mid_1, Mid.inner_1), inner_alias, join))
        with self.subTest("mid_1_inner_2"):
            inner_alias = pk.Table("inner").as_("_4_inner")
            join = alias.inner_2_key == inner_alias.key
            self.assertEqual(joins[3], ((Outer.mid_1, Mid.inner_2), inner_alias, join))
        with self.subTest("size"):
            self.assertEqual(len(joins), 4)

    def test_repr(self):
        class Other(Table):
            key = Field()
        class Model(Table):
            field = Field()
            other_key = Field(foreign=Other.key)
            other = Reference(other_key, Other)
        inst = Model(field="value", other_key=1)
        setattr(inst, "other", Other(key=1))
        self.assertEqual(repr(inst), "Model(field='value', other_key=1, other=Other(key=1))")
