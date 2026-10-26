"""Route ``hdfs://`` polars reads through a local hdfs-s3-gateway."""

from __future__ import annotations

import inspect
import os
import secrets
import threading
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl

from . import _patch

_BUCKET = "hdfs"
# Loopback only: the gateway is a local permission boundary, not a network service.
_LISTEN_ADDR = "127.0.0.1:0"
_SECRET_BYTES = 32

# Only wrap functions that accept storage_options; the rest cannot configure HDFS.
_FUNCTIONS = (
    "read_parquet",
    "scan_parquet",
    "read_csv",
    "scan_csv",
    "read_ipc",
    "scan_ipc",
    "read_ndjson",
    "scan_ndjson",
    "read_lines",
    "scan_lines",
    "read_delta",
    "scan_delta",
)
_ReaderKey = tuple[str, str | None, tuple[tuple[str, str], ...], str | None]


@dataclass(frozen=True)
class _Reader:
    """A running gateway plus the secret polars signs its requests with."""

    instance: Any
    secret: str


_readers: dict[_ReaderKey, _Reader] = {}
_lock = threading.Lock()


def register() -> None:
    """Patch polars' read/scan functions. Idempotent."""
    for name in _FUNCTIONS:
        fn = getattr(pl, name, None)
        if fn is not None and _accepts_storage_options(fn):
            _patch.patch(fn, name, _translate_call)


def unregister() -> None:
    """Undo :func:`register`."""
    _patch.unregister()


def shutdown() -> None:
    """Stop all HDFS readers started by the plugin."""
    with _lock:
        readers = [reader.instance for reader in _readers.values()]
        _readers.clear()
    for reader in readers:
        with suppress(Exception):
            reader.stop()


def _accepts_storage_options(fn: Any) -> bool:
    try:
        return "storage_options" in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def _translate_call(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    # Both positional and keyword source is a TypeError in polars; pass it through
    # so the original call raises it, rather than the patch dropping one argument.
    if args and "source" in kwargs:
        return args, kwargs
    if args:
        source = args[0]
    elif "source" in kwargs:
        source = kwargs["source"]
    else:
        return args, kwargs

    translated = _translate(source)
    if translated is None:
        return args, kwargs

    namenode, s3_uri = translated
    storage_options = dict(kwargs.pop("storage_options", None) or {})
    endpoint, secret = _endpoint(
        namenode,
        user=storage_options.pop("hdfs_user", None),
        options=storage_options.pop("hdfs_options", None),
        config_dir=storage_options.pop("hdfs_config_dir", None),
    )
    # The gateway requires a valid SigV4 signature once its secret is set, so a
    # user-supplied anonymous mode is dropped rather than honoured.
    storage_options.pop("aws_skip_signature", None)
    storage_options.update(
        aws_access_key_id="polars-hdfs",
        aws_secret_access_key=secret,
        aws_allow_http="true",
        aws_endpoint_url=endpoint,
    )
    if args:
        args = (s3_uri, *args[1:])
    else:
        kwargs["source"] = s3_uri
    kwargs["storage_options"] = storage_options
    return args, kwargs


def _translate(source: Any) -> tuple[str | None, str] | None:
    if isinstance(source, str):
        if source[:7].lower() != "hdfs://":
            return None
        parts = urlsplit(source)
        if parts.query or parts.fragment:
            raise ValueError("hdfs:// URIs may not contain a query string or fragment")
        if parts.username is not None or parts.password is not None:
            raise ValueError(
                "user info is not supported in hdfs:// URIs; pass hdfs_user via storage_options"
            )

        namenode = f"hdfs://{parts.netloc}" if parts.netloc else None
        # The path is passed through literally, the way polars reads local and
        # s3:// keys: polars percent-encodes the key itself when it builds the
        # request, so encoding here would apply twice (`year=2025` would be sent
        # as `year%253D2025` and find nothing) and a name holding a literal
        # `%3D` would be decoded into a different object.
        return namenode, f"s3://{_BUCKET}/{parts.path.lstrip('/')}"

    if isinstance(source, list | tuple) and any(
        isinstance(item, str) and item[:7].lower() == "hdfs://" for item in source
    ):
        raise ValueError("hdfs:// sources in lists are not supported; use a directory or glob")
    return None


def _endpoint(
    namenode: str | None,
    *,
    user: str | None,
    options: Mapping[str, str] | None,
    config_dir: str | os.PathLike[str] | None = None,
) -> tuple[str, str]:
    """Return the gateway's address and the secret requests must be signed with."""
    if user is not None and not isinstance(user, str):
        raise TypeError("hdfs_user must be a string")
    frozen_options = _freeze_options(options)
    resolved_config_dir = _resolve_config_dir(config_dir)
    # With no authority, hdfs-native resolves fs.defaultFS from the Hadoop config.
    resolved_namenode = namenode or "hdfs://"

    key = (resolved_namenode, user, frozen_options, resolved_config_dir)
    with _lock:
        reader = _readers.get(key)
        if reader is None:
            # A fresh secret per reader, so another local process cannot reuse the
            # credentials of a gateway it did not start.
            secret = secrets.token_urlsafe(_SECRET_BYTES)
            reader = _Reader(
                instance=_start_reader(
                    resolved_namenode,
                    user,
                    frozen_options,
                    resolved_config_dir,
                    secret=secret,
                ),
                secret=secret,
            )
            _readers[key] = reader

    return f"http://{reader.instance.address}", reader.secret


def _start_reader(
    namenode: str,
    user: str | None,
    options: tuple[tuple[str, str], ...],
    config_dir: str | None,
    *,
    secret: str,
) -> Any:
    # Deferred: importing hdfs_s3_gateway loads a native module that registers an
    # atexit hook, so only pay for it on an actual hdfs:// read.
    from hdfs_s3_gateway import gateway

    # Everything is passed as keywords: the secret and the HDFS settings never
    # leave memory, so there is no config file to write or clean up.
    instance = gateway(
        namenode_uri=namenode,
        hdfs_root="/",
        bucket_name=_BUCKET,
        listen_addr=_LISTEN_ADDR,
        auth_secret=secret,
        hdfs_user=user,
        hdfs_config_dir=config_dir,
        hdfs_options=dict(options),
    )

    try:
        instance.start()
    except Exception:
        with suppress(Exception):
            instance.stop()
        raise
    return instance


def _freeze_options(options: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    if options is None:
        return ()
    try:
        items = options.items()
    except AttributeError as exc:
        raise TypeError("hdfs_options must be a mapping of strings to strings") from exc

    frozen: list[tuple[str, str]] = []
    for key, value in items:
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("hdfs_options must be a mapping of strings to strings")
        frozen.append((key, value))
    return tuple(sorted(frozen))


def _resolve_config_dir(config_dir: str | os.PathLike[str] | None) -> str | None:
    if config_dir is not None:
        path = os.fsdecode(config_dir)
        if path:
            return _canonical_path(path)
    if env_dir := os.environ.get("HADOOP_CONF_DIR"):
        return _canonical_path(env_dir)
    if hadoop_home := os.environ.get("HADOOP_HOME"):
        return _canonical_path(os.path.join(hadoop_home, "etc", "hadoop"))
    return None


def _canonical_path(path: str) -> str:
    return str(Path(path).expanduser().resolve())
