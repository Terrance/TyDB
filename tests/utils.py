import asyncio
from inspect import isfunction
import json
import os
import sqlite3
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type, Union
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
from tydb.session import AsyncSession, Session
from tydb.utils import maybe_await


AnySession = Union[Session, AsyncSession]
AnySessionFactory = Callable[[], Union[Awaitable[AnySession], AnySession]]

SessionTestMethod = Callable[[TestCase, AnySession], Awaitable[None]]
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
    loop: asyncio.AbstractEventLoop, fn: SessionTestMethod, sessions: Dict[str, AnySession], *tables: Type[Table],
) -> Tuple[TestMethod, ...]:
    async def run_test(self: TestCase, key: str, sess_factory: AnySessionFactory) -> None:
        try:
            sess = sessions[key]
        except KeyError:
            sess = sessions.setdefault(key, await maybe_await(sess_factory()))
        await maybe_await(sess.setup(*tables))
        try:
            await fn(self, sess)
        finally:
            await maybe_await(sess.destroy(*tables))
    def sqlite(self: TestCase):
        loop.run_until_complete(run_test(self, "sqlite", lambda: Session(sqlite3.connect(":memory:"), SQLiteDialect)))
    def postgresql(self: TestCase):
        if not psycopg:
            self.skipTest("No PostgreSQL driver installed (psycopg)")
        pgsql_conn = os.getenv("TYDB_PGSQL_CONN")
        if not pgsql_conn:
            self.skipTest("No PostgreSQL connection configured (TYDB_PGSQL_CONN)")
        loop.run_until_complete(run_test(self, "postgresql", lambda: Session(psycopg.connect(**json.loads(pgsql_conn)), PostgreSQLDialect)))
    def mysql(self: TestCase):
        if not pymysql:
            self.skipTest("No MySQL driver installed (pymysql)")
        mysql_conn = os.getenv("TYDB_MYSQL_CONN")
        if not mysql_conn:
            self.skipTest("No MySQL connection configured (TYDB_MYSQL_CONN)")
        loop.run_until_complete(run_test(self, "mysql", lambda: Session(pymysql.connect(**json.loads(mysql_conn)), MySQLDialect)))
    def sqlite_async(self: TestCase):
        if not os.getenv("TYDB_ASYNC"):
            self.skipTest("No async tests enabled (TYDB_ASYNC)")
        if not aiosqlite:
            self.skipTest("No async SQLite driver installed (aiosqlite)")
        async def factory():
            return AsyncSession(await aiosqlite.connect(":memory:"), SQLiteDialect)
        loop.run_until_complete(run_test(self, "sqlite", factory))
    def postgresql_async(self: TestCase):
        if not os.getenv("TYDB_ASYNC"):
            self.skipTest("No async tests enabled (TYDB_ASYNC)")
        if not aiopg:
            self.skipTest("No async PostgreSQL driver installed (aiopg)")
        pgsql_conn = os.getenv("TYDB_PGSQL_CONN")
        if not pgsql_conn:
            self.skipTest("No PostgreSQL connection configured (TYDB_PGSQL_CONN)")
        async def factory():
            return AsyncSession(await aiopg.connect(**json.loads(pgsql_conn)), PostgreSQLDialect)
        loop.run_until_complete(run_test(self, "sqlite", factory))
    def mysql_async(self: TestCase):
        if not os.getenv("TYDB_ASYNC"):
            self.skipTest("No async tests enabled (TYDB_ASYNC)")
        if not aiomysql:
            self.skipTest("No async MySQL driver installed (aiomysql)")
        mysql_conn = os.getenv("TYDB_MYSQL_CONN")
        if not mysql_conn:
            self.skipTest("No MySQL connection configured (TYDB_MYSQL_CONN)")
        async def factory():
            return AsyncSession(await aiomysql.connect(**json.loads(mysql_conn)), MySQLDialect)
        loop.run_until_complete(run_test(self, "sqlite", factory))
    return sqlite, postgresql, mysql, sqlite_async, postgresql_async, mysql_async


def with_dialects(*tables: Type[Table]):
    def outer(cls: Type[TestCase]):
        loop = asyncio.get_event_loop()
        setup: Set[Type[Table]] = set(tables)
        functions: Dict[str, Tuple[TestMethod, ...]] = {}
        sessions: Dict[str, AnySession] = {}
        tear_down: Optional[Callable[[], None]] = getattr(cls, "tearDownClass")
        def tearDownClass(cls):
            if tear_down:
                tear_down()
            for sess in sessions.values():
                loop.run_until_complete(maybe_await(sess.conn.close()))
        cls.tearDownClass = classmethod(tearDownClass)
        for name, member in vars(cls).items():
            if isinstance(member, type) and issubclass(member, Table):
                setup.add(member)
            elif isfunction(member):
                functions[name] = dialect_methods(loop, member, sessions, *setup)
        for name, methods in tuple(functions.items()):
            for method in methods:
                setattr(cls, "{}__{}".format(name, method.__name__), method)
            delattr(cls, name)
        return cls
    return outer
