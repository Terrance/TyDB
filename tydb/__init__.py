"""
.. include:: ../README.md

## Basic usage

Declare your tables as regular Python classes with descriptor-based fields:

```python
from tydb.fields import IntField, Nullable, StrField
from tydb.models import Default, Table

class Item(Table, primary="id"):
    id = IntField(default=Default.SERVER)
    name = StrField()
    desc = Nullable.StrField()
```

Access and manage records using a session:

```python
import sqlite3

from tydb.dialects import SQLiteDialect
from tydb.session import Session

def main():
    conn = sqlite3.connect(":memory:")
    sess = Session(conn, SQLiteDialect)

    sess.create(Item, name="Test")  # Item(id=1, name='Test')
    for item in sess.select(Item, Item.name == "Test"):  # <SelectQueryResult: ...>
        sess.remove(item)
```

In an asynchronous workflow, not much changes:

```python
import aiosqlite

from tydb.dialects import SQLiteDialect
from tydb.session import AsyncSession

async def main():
    conn = await aiosqlite.connect(":memory:")
    sess = AsyncSession(conn, SQLiteDialect)

    await sess.create(Item, name="Test")  # Item(id=1, name='Test')
    async for item in await sess.select(Item, Item.name == "Test"):  # <AsyncSelectQueryResult: ...>
        await  sess.remove(item)
```

## Expressions

Fields of a model can be used to create expressions, used as `WHERE` clauses in queries:

```python
>>> Item.name == "Test"
<Expr: eq (<StrField: Item.name>, 'Test')>
>>> Item.id @ (1, 2, 3)
<Expr: isin (<IntField: Item.id>, (1, 2, 3))>
>>> (Item.desc != None) & (Item.desc ** "%stuff%")
<Expr: and_ (<Expr: isnotnull (<StrField: Item.desc>)>,
             <Expr: ilike (<StrField: Item.desc>, '%stuff%')>)>
```
"""
