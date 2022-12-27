from inspect import isawaitable
from typing import Awaitable, TypeVar, Union


_T = TypeVar("_T")


async def maybe_await(result: Union[Awaitable[_T], _T]) -> _T:
    if isawaitable(result):
        return await result
    else:
        return result
