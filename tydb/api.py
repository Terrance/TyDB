"""
Partial typing protocols for DB-API 2.0.
"""

from typing import Any, Awaitable, Iterable, Optional, Tuple, TypeVar, Union

from typing_extensions import Protocol


_T = TypeVar("_T")
_MaybeAsync = Union[_T, Awaitable[_T]]


class Cursor(Protocol):
    def close(self) -> None: ...
    def execute(self, operation: Any, parameters: Iterable[Any] = ...) -> Any: ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]: ...
    lastrowid: Optional[int]


class Connection(Protocol):
    def cursor(self) -> Cursor: ...


class AsyncCursor(Protocol):
    def close(self) -> _MaybeAsync[None]: ...
    def execute(self, operation: Any, parameters: Iterable[Any] = ...) -> _MaybeAsync[Any]: ...
    def fetchone(self) -> _MaybeAsync[Optional[Tuple[Any, ...]]]: ...
    lastrowid: Optional[int]


class AsyncConnection(Protocol):
    def cursor(self) -> _MaybeAsync[AsyncCursor]: ...
