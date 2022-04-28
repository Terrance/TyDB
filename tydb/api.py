from typing import Any, Iterable, Optional, Protocol, Tuple


class Cursor(Protocol):
    def execute(self, operation: Any, parameters: Iterable[Any] = ..., /) -> Any: ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...
