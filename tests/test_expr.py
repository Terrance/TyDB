from datetime import datetime
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
    ('"int"=1', Model.int == 1),
    ('"int"<>1', Model.int != 1),
    ('"int">1', Model.int > 1),
    ('"int">=1', Model.int >= 1),
    ('"int"<1', Model.int < 1),
    ('"int"<=1', Model.int <= 1),
    ('-"int"=1', -Model.int == 1),
    ('"int" IN (1,2)', Model.int @ (1, 2)),
    ('"bool"=true', Model.bool == True),
    ('"float"<1.23', Model.float < 1.23),
    ('"str" LIKE \'%match%\'', Model.str * "%match%"),
    ('"str" ILIKE \'%match%\'', Model.str ** "%match%"),
    ('"str" IS NULL', Model.str == None),
    ('"str" IS NOT NULL', Model.str != None),
    ('"date"=\'{}\''.format(NOW.isoformat()), Model.date == NOW),
    ('"int"=1 OR "int"=2', (Model.int == 1) | (Model.int == 2)),
    ('"int">=1 AND "int"<=2', (Model.int >= 1) & (Model.int <= 2)),
    ('("int"=1 OR "int"=2) AND "str"=\'A\'', ((Model.int == 1) | (Model.int == 2)) & (Model.str == "A")),
    ('NOT "int"=1', ~(Model.int == 1)),
)
class TestExpr(TestCase):
    
    def test_expr(self, sql: str, expr: Expr):
        self.assertEqual(str(expr.pk_frag), sql)
