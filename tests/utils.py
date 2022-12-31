import asyncio
from inspect import isfunction
import json
import os
import sqlite3
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Tuple, Type, Union
from unittest import TestCase

try:
    import aiomysql
except ImportError:
    aiomysql = None

try:
    import aiopg
except ImportError:
    aiopg = None

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

try:
    import pymysql
except ImportError:
    pymysql = None

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


def parametised_methods(fn: Callable[..., None], *matrix: Iterable[Any]) -> Iterable[TestMethod]:
    for values in matrix:
        def run(self: TestCase, *args):
            return fn(self, *args, *values)
        yield run


def parametise(*matrix: Iterable[Any]):
    def outer(cls: Type[TestCase]):
        found: Dict[str, Iterable[TestMethod]] = {}
        for name, member in vars(cls).items():
            if isfunction(member):
                found[name] = parametised_methods(member, *matrix)
        for name, methods in found.items():
            for i, method in enumerate(methods):
                setattr(cls, "{}__{}".format(name, i), method)
            delattr(cls, name)
        return cls
    return outer


def dialect_methods(
    fn: SessionTestMethod, *tables: Type[Table]
) -> Tuple[TestMethod, ...]:
    async def run_test(self: TestCase, sess: Union[Session, AsyncSession]):
        await maybe_await(sess.setup(*tables))
        try:
            await fn(self, sess)
        finally:
            await maybe_await(sess.destroy(*tables))
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
    def mysql(self: TestCase):
        if not pymysql:
            self.skipTest("No MySQL driver installed (pymysql)")
        mysql_conn = os.getenv("TYDB_MYSQL_CONN")
        if not mysql_conn:
            self.skipTest("No MySQL connection configured (TYDB_MYSQL_CONN)")
        with pymysql.connect(**json.loads(mysql_conn)) as conn:
            asyncio.run(run_test(self, Session(conn, MySQLDialect)))
    def sqlite_async(self: TestCase):
        if not aiosqlite:
            self.skipTest("No async SQLite driver installed (aiosqlite)")
        async def inner():
            async with aiosqlite.connect(":memory:") as conn:
                await run_test(self, AsyncSession(conn, SQLiteDialect))
        asyncio.run(inner())
    def postgresql_async(self: TestCase):
        if not aiopg:
            self.skipTest("No async PostgreSQL driver installed (aiopg)")
        pgsql_conn = os.getenv("TYDB_PGSQL_CONN")
        if not pgsql_conn:
            self.skipTest("No PostgreSQL connection configured (TYDB_PGSQL_CONN)")
        async def inner():
            async with aiopg.connect(pgsql_conn) as conn:
                await run_test(self, AsyncSession(conn, PostgreSQLDialect))
        asyncio.run(inner())
    def mysql_async(self: TestCase):
        if not aiomysql:
            self.skipTest("No async MySQL driver installed (aiomysql)")
        mysql_conn = os.getenv("TYDB_MYSQL_CONN")
        if not mysql_conn:
            self.skipTest("No MySQL connection configured (TYDB_MYSQL_CONN)")
        async def inner():
            async with aiomysql.connect(**json.loads(mysql_conn)) as conn:
                await run_test(self, AsyncSession(conn, MySQLDialect))
        asyncio.run(inner())
    return sqlite, postgresql, mysql, sqlite_async, postgresql_async, mysql_async


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
