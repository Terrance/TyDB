import asyncio
from inspect import isfunction
import json
import os
import sqlite3
from typing import Awaitable, Callable, Dict, List, Tuple, Type, Union
from unittest import TestCase

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

try:
    import MySQLdb
except ImportError:
    MySQLdb = None

try:
    import psycopg
except ImportError:
    psycopg = None

from tydb.dialects import MySQLDialect, PostgreSQLDialect, SQLiteDialect
from tydb.models import Table
from tydb.session import Session, AsyncSession
from tydb.utils import maybe_await


SessionTestMethod = Callable[[TestCase, Union[AsyncSession, Session]], Awaitable[None]]
TestMethod = Callable[[TestCase], None]


def dialect_methods(
    fn: SessionTestMethod, *tables: Type[Table]
) -> Tuple[TestMethod, ...]:
    async def run_test(self: TestCase, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.setup(*tables))
        await fn(self, sess)
    def sqlite(self: TestCase):
        with sqlite3.connect(":memory:") as conn:
            asyncio.run(run_test(self, Session(conn, SQLiteDialect)))
    def postgresql(self: TestCase):
        if not psycopg:
            self.skipTest("No PostgreSQL driver installed (psycopg)")
        pgsql_conn = os.getenv("TYDB_PGSQL_CONN")
        if not pgsql_conn:
            self.skipTest("No PostgreSQL connection configured (TYDB_PGSQL_CONN)")
        with psycopg.connect(pgsql_conn) as conn:
            asyncio.run(run_test(self, Session(conn, PostgreSQLDialect)))
            conn.rollback()
    def mysql(self: TestCase):
        if not MySQLdb:
            self.skipTest("No MySQL driver installed (MySQLdb)")
        mysql_conn = os.getenv("TYDB_MYSQL_CONN")
        if not mysql_conn:
            self.skipTest("No MySQL connection configured (TYDB_MYSQL_CONN)")
        with MySQLdb.connect(**json.loads(mysql_conn)) as conn:
            asyncio.run(run_test(self, Session(conn, MySQLDialect)))
            conn.rollback()
    def sqlite_async(self: TestCase):
        if not aiosqlite:
            self.skipTest("No async SQLite driver installed (aiosqlite)")
        async def inner():
            async with aiosqlite.connect(":memory:") as conn:
                await run_test(self, AsyncSession(conn, SQLiteDialect))
        asyncio.run(inner())
    return sqlite, postgresql, mysql, sqlite_async


def with_dialects(cls: Type[TestCase]) -> Type[TestCase]:
    tables: List[Type[Table]] = []
    found: Dict[str, Tuple[TestMethod, TestMethod, TestMethod]] = {}
    for name, member in vars(cls).items():
        if isinstance(member, type) and issubclass(member, Table):
            tables.append(member)
        elif isfunction(member):
            found[name] = dialect_methods(member, *tables)
    for name, methods in tuple(found.items()):
        for method in methods:
            setattr(cls, "{}__{}".format(name, method.__name__), method)
        delattr(cls, name)
    return cls
