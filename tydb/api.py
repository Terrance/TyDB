from typing import Any, Awaitable, Iterable, Optional, Protocol, Tuple, TypeVar, Union


_T = TypeVar("_T")
_MaybeAsync = Union[_T, Awaitable[_T]]


class Cursor(Protocol):
    def execute(self, operation: Any, parameters: Iterable[Any] = ..., /) -> Any: ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]: ...
    lastrowid: Optional[int]


class Connection(Protocol):
    def cursor(self) -> Cursor: ...


class AsyncCursor(Protocol):
    def execute(self, operation: Any, parameters: Iterable[Any] = ..., /) -> _MaybeAsync[Any]: ...
    def fetchone(self) -> _MaybeAsync[Optional[Tuple[Any, ...]]]: ...
    lastrowid: Optional[int]


class AsyncConnection(Protocol):
    def cursor(self) -> _MaybeAsync[AsyncCursor]: ...
