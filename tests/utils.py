import asyncio
from inspect import isfunction
import json
import os
import sqlite3
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Set, Tuple, Type, Union
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


# Separate method to avoid variable reassignment in closure
def parametised_method(fn: Callable[..., None], *values: Any) -> TestMethod:
    def run(self: TestCase, *args):
        return fn(self, *args, *values)
    return run


def parametise(*matrix: Iterable[Any]):
    def outer(cls: Type[TestCase]):
        found: Dict[str, List[TestMethod]] = {}
        for name, member in vars(cls).items():
            if isfunction(member):
                found[name] = []
                for values in matrix:
                    found[name].append(parametised_method(member, *values))
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
        with psycopg.connect(**json.loads(pgsql_conn)) as conn:
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
            async with aiopg.connect(**json.loads(pgsql_conn)) as conn:
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


def with_dialects(*tables: Type[Table]):
    def outer(cls: Type[TestCase]):
        setup: Set[Type[Table]] = set(tables)
        functions: Dict[str, Tuple[TestMethod, ...]] = {}
        for name, member in vars(cls).items():
            if isinstance(member, type) and issubclass(member, Table):
                setup.add(member)
            elif isfunction(member):
                functions[name] = dialect_methods(member, *setup)
        for name, methods in tuple(functions.items()):
            for method in methods:
                setattr(cls, "{}__{}".format(name, method.__name__), method)
            delattr(cls, name)
        return cls
    return outer
