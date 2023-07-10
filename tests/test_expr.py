from datetime import datetime
from typing import Callable
from unittest import TestCase

from tydb.fields import BoolField, DateTimeField, FloatField, IntField, StrField
from tydb.models import Expr, Table

try:
    from .utils import parametise
except ImportError:
    from tests.utils import parametise


class Model(Table, primary="int"):
    int = IntField()
    float = FloatField()
    bool = BoolField()
    str = StrField()
    date = DateTimeField()


NOW = datetime.now().astimezone()


@parametise(
    ('"int"=1', lambda: Model.int == 1),
    ('"int"<>1', lambda: Model.int != 1),
    ('"int">1', lambda: Model.int > 1),
    ('"int">=1', lambda: Model.int >= 1),
    ('"int"<1', lambda: Model.int < 1),
    ('"int"<=1', lambda: Model.int <= 1),
    ('-"int"=1', lambda: -Model.int == 1),
    ('"int" IN (1,2)', lambda: Model.int @ (1, 2)),
    ('"bool"=true', lambda: Model.bool == True),
    ('"float"<1.23', lambda: Model.float < 1.23),
    ('"str" LIKE \'%match%\'', lambda: Model.str * "%match%"),
    ('"str" ILIKE \'%match%\'', lambda: Model.str ** "%match%"),
    ('"str" IS NULL', lambda: Model.str == None),
    ('"str" IS NOT NULL', lambda: Model.str != None),
    ('"date"=\'{}\''.format(NOW.isoformat()), lambda: Model.date == NOW),
    ('"int"=1 OR "int"=2', lambda: (Model.int == 1) | (Model.int == 2)),
    ('"int">=1 AND "int"<=2', lambda: (Model.int >= 1) & (Model.int <= 2)),
    ('("int"=1 OR "int"=2) AND "str"=\'A\'', lambda: ((Model.int == 1) | (Model.int == 2)) & (Model.str == "A")),
    ('NOT "int"=1', lambda: ~(Model.int == 1)),
)
class TestExpr(TestCase):
    
    def test_expr(self, sql: str, expr: Callable[[], Expr]):
        self.assertEqual(str(expr().pk_frag), sql)
