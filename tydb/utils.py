from inspect import isawaitable
from typing import Any, Awaitable, Callable, Coroutine, Optional, ParamSpec, TypeVar, Union


_T = TypeVar("_T")
_P = ParamSpec("_P")


async def maybe_await(result: Union[Awaitable[_T], _T]) -> _T:
    if isawaitable(result):
        return await result
    else:
        return result
