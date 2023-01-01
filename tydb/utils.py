"""
Miscellaneous helper methods.
"""

from inspect import isawaitable
from typing import Awaitable, TypeVar, Union


_T = TypeVar("_T")


async def maybe_await(result: Union[Awaitable[_T], _T]) -> _T:
    """
    Handle a potentially-awaitable return value by `await`ing it if it's an `Awaitable`.

    Can be used with functions that may or may not be coroutines.
    """
    if isawaitable(result):
        return await result
    else:
        return result
