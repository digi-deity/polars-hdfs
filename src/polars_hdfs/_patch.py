"""Patch Python functions in place so every existing reference sees the wrapper.

Only Python functions are supported.  That is all this package needs: the
polars readers are ordinary Python functions, and swapping ``__code__`` keeps
their identity, signature, docstring, and decorator behaviour intact.
"""

from __future__ import annotations

import inspect
import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

_DISPATCH = "__polars_hdfs__"
_SCRIPT = "<polars_hdfs>"

Translate = Callable[
    [tuple[Any, ...], dict[str, Any]],
    tuple[tuple[Any, ...], dict[str, Any]],
]


@dataclass
class _Patch:
    function: types.FunctionType
    code: types.CodeType
    defaults: tuple[Any, ...] | None
    kwdefaults: dict[str, Any] | None
    key: str


_targets: dict[str, Callable[..., Any]] = {}
_patches: dict[int, _Patch] = {}
_namespaces: list[dict[str, Any]] = []


def patch(fn: Any, key: str, translate: Translate) -> None:
    """Replace ``fn``'s body with a call through ``translate`` and a clone."""
    code = getattr(fn, "__code__", None)
    namespace = getattr(fn, "__globals__", None)
    if (
        not isinstance(code, types.CodeType)
        or not isinstance(namespace, dict)
        or id(fn) in _patches
        or code.co_filename == _SCRIPT
        or code.co_flags
        & (inspect.CO_GENERATOR | inspect.CO_COROUTINE | inspect.CO_ASYNC_GENERATOR)
    ):
        return

    try:
        clone = types.FunctionType(
            code, namespace, fn.__name__, fn.__defaults__, fn.__closure__
        )
        new_code = _generated_code(code, key)
    except (TypeError, ValueError, SyntaxError):
        return

    clone.__kwdefaults__ = fn.__kwdefaults__

    def call(*args: Any, **kwargs: Any) -> Any:
        args, kwargs = translate(args, kwargs)
        return clone(*args, **kwargs)

    _targets[key] = call
    namespace[_DISPATCH] = _targets
    if not any(namespace is ns for ns in _namespaces):
        _namespaces.append(namespace)

    _patches[id(fn)] = _Patch(fn, code, fn.__defaults__, fn.__kwdefaults__, key)
    fn.__code__ = new_code
    fn.__defaults__ = None
    fn.__kwdefaults__ = None


def unregister() -> None:
    """Restore every patched function."""
    for state in reversed(_patches.values()):
        state.function.__code__ = state.code
        state.function.__defaults__ = state.defaults
        state.function.__kwdefaults__ = state.kwdefaults
        _targets.pop(state.key, None)
    _patches.clear()

    for namespace in _namespaces:
        if namespace.get(_DISPATCH) is _targets:
            del namespace[_DISPATCH]
    _namespaces.clear()


def _generated_code(code: types.CodeType, key: str) -> types.CodeType:
    source = (
        "def _hook(*args, **kwargs):\n"
        f"    return {_DISPATCH}[{key!r}](*args, **kwargs)\n"
    )
    namespace: dict[str, Any] = {}
    exec(compile(source, _SCRIPT, "exec"), namespace)
    generated: types.CodeType = namespace["_hook"].__code__
    return generated.replace(
        co_freevars=tuple(code.co_freevars),
        co_name=code.co_name,
    )
