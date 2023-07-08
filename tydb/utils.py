"""
Miscellaneous helper methods.
"""

from inspect import isawaitable
from typing import Any, Awaitable, Type, TypeVar, Union

from .models import _Descriptor


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


def resolve_late_descriptors(*targets: Type[Any]):
    """
    Assign missing `owner`/`name` attributes to descriptors assigned after class creation (e.g.
    when creating self-referential fields).
    """
    for target in targets:
        for attr, value in vars(target).items():
            if isinstance(value, _Descriptor) and not hasattr(value, "owner"):
                value.__set_name__(target, attr)
