# TyDB

A simple, type-friendly Python ORM.

## Features

Things one might expect of any ORM:

* Lightweight class-based models and descriptor-based fields
* Built-in fields for common types
* Primary keys
* Foreign keys with attribute accessors and joined queries
* Separate nullable fields

Things one might not:

* Reliable type signatures on the public API
* Support for any [DB-API 2.0](https://peps.python.org/pep-0249/) database driver
* Synchronous and asynchronous operation under a common API

## Database support

Low-level interaction with databases is handled by a [DB-API `Connection`](https://peps.python.org/pep-0249/#connection-objects).  This means you can work with any type of database, as long as a database driver exists that implements a DB-API interface -- Python's built-in SQLite library does so, whilst external modules are available for MySQL, PostgreSQL and others.

Whilst there's no asynchronous DB-API specification, some database drivers implement a DB-API-like interface with the same methods presented as coroutines; these are supported with asynchronous sessions, which will attempt to await results of `Connection` method calls if they return awaitables.

## Unit tests

The included tests can be ran using:

```shell
$ make test
```

By default, this will just run generic tests, and SQLite tests against an in-memory database.

To run against MySQL or PostgreSQL, you'll need to install the database drivers used by the tests:

```shell
$ pip install -r requirements-dev.txt
```

Credentials must also be provided to connect to live database servers.  These should be JSON-formatted strings containing `kwargs` for the underlying driver connection methods.  For example, to connect to servers listening locally without authentication:

```shell
$ export TYDB_MYSQL_CONN='{"db": "tydb"}'
$ export TYDB_PGSQL_CONN='{"dbname": "tydb"}'
```

Only synchronous session and driver tests are run by default. To enable asynchronous tests too:

```shell
$ export TYDB_ASYNC=1
```

The dialect-aware tests currently use the following databases and drivers:

* MySQL
  * [`pymysql`](https://pypi.org/project/pymysql/)
  * [`aiomysql`](https://pypi.org/project/aiomysql/)
* PostgreSQL
  * [`psycopg`](https://pypi.org/project/psycopg/)
  * [`aiopg`](https://pypi.org/project/aiopg/)
* SQLite
  * [`sqlite3`](https://docs.python.org/3/library/sqlite3.html) (built-in)
  * [`aiosqlite`](https://pypi.org/project/aiosqlite/)

## Docs

These can be built using [pdoc](https://pdoc.dev), assuming you've installed the dev requirements already:

```shell
$ make docs
```
