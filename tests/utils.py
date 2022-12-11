from functools import wraps
from inspect import isfunction
import json
import os
import sqlite3
from typing import Callable, Dict, List, Tuple, Type
from unittest import TestCase

try:
    import MySQLdb
except ImportError:
    MySQLdb = None

try:
    import psycopg
except ImportError:
    psycopg = None

from tydb.api import Connection
from tydb.dialects import Dialect, MySQLDialect, PostgreSQLDialect, SQLiteDialect
from tydb.models import Table
from tydb.session import Session


SessionTestMethod = Callable[[TestCase, Session], None]
TestMethod = Callable[[TestCase], None]


def dialect_methods(
    fn: SessionTestMethod, *tables: Type[Table]
) -> Tuple[TestMethod, TestMethod, TestMethod]:
    def run_test(self: TestCase, conn: Connection, dialect: Type[Dialect]):
        sess = Session(conn, dialect)
        sess.setup(*tables)
        fn(self, sess)
    @wraps(fn)
    def sqlite(self: TestCase):
        with sqlite3.connect(":memory:") as conn:
            run_test(self, conn, SQLiteDialect)
    @wraps(fn)
    def postgresql(self: TestCase):
        if not psycopg:
            self.skipTest("No PostgreSQL driver installed (psycopg)")
        pgsql_conn = os.getenv("TYDB_PGSQL_CONN")
        if not pgsql_conn:
            self.skipTest("No PostgreSQL connection configured (TYDB_PGSQL_CONN)")
        with psycopg.connect(pgsql_conn) as conn:
            run_test(self, conn, PostgreSQLDialect)
            conn.rollback()
    @wraps(fn)
    def mysql(self: TestCase):
        if not MySQLdb:
            self.skipTest("No MySQL driver installed (MySQLdb)")
        mysql_conn = os.getenv("TYDB_MYSQL_CONN")
        if not mysql_conn:
            self.skipTest("No MySQL connection configured (TYDB_MYSQL_CONN)")
        with MySQLdb.connect(**json.loads(mysql_conn)) as conn:
            run_test(self, conn, MySQLDialect)
            conn.rollback()
    return sqlite, postgresql, mysql


def with_dialects(cls: Type[TestCase]) -> Type[TestCase]:
    tables: List[Type[Table]] = []
    found: Dict[str, Tuple[TestMethod, TestMethod, TestMethod]] = {}
    for name, member in vars(cls).items():
        if isinstance(member, type) and issubclass(member, Table):
            tables.append(member)
        elif isfunction(member):
            found[name] = dialect_methods(member, *tables)
    for name, methods in tuple(found.items()):
        methods = zip(("sqlite", "postgresql", "mysql"), methods)
        for dialect, method in methods:
            setattr(cls, "{}__{}".format(name, dialect), method)
        delattr(cls, name)
    return cls
