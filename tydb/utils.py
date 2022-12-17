from inspect import isawaitable
from typing import Awaitable, Callable, ParamSpec, TypeVar, Union


_T = TypeVar("_T")
_P = ParamSpec("_P")


async def maybe_await(
    func: Callable[_P, Union[_T, Awaitable[_T]]], *args: _P.args, **kwargs: _P.kwargs,
) -> _T:
    result = func(*args, **kwargs)
    if isawaitable(result):
        return await result
    else:
        return result
