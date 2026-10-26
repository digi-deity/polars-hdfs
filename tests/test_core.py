from __future__ import annotations

import inspect
import sys
import traceback
import types
from pathlib import Path
from typing import Any

import polars as pl
import polars.exceptions as polars_exceptions
import pytest

from polars_hdfs import _core

# Added in polars 2.0. Older versions cannot run the removed-parameter test, so
# it is skipped rather than failing at import time.
_ARGUMENT_REMOVED_ERROR = getattr(polars_exceptions, "ArgumentRemovedError", None)


class FakeGateway:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        config_path = kwargs.get("config_path") or (args[0] if args else None)
        self.config = Path(config_path).read_text(encoding="utf-8") if config_path else None
        self.address = "127.0.0.1:4321"
        self.stopped = False

    def start(self) -> None:
        pass

    def stop(self) -> None:
        self.stopped = True


def install_fake(monkeypatch: pytest.MonkeyPatch) -> list[FakeGateway]:
    created: list[FakeGateway] = []

    def gateway(*args: Any, **kwargs: Any) -> FakeGateway:
        instance = FakeGateway(*args, **kwargs)
        created.append(instance)
        return instance

    module = types.ModuleType("hdfs_s3_gateway")
    module.gateway = gateway  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "hdfs_s3_gateway", module)
    return created


def test_uri_authority_starts_and_reuses_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake(monkeypatch)

    endpoint, secret = _core._endpoint("hdfs://nn:8020", user=None, options=None)
    assert endpoint == "http://127.0.0.1:4321"
    assert len(secret) >= _core._SECRET_BYTES
    assert created[0].kwargs == {
        "namenode_uri": "hdfs://nn:8020",
        "hdfs_root": "/",
        "bucket_name": "hdfs",
        "listen_addr": "127.0.0.1:0",
        "auth_secret": secret,
    }

    assert _core._endpoint("hdfs://nn:8020", user=None, options=None) == (endpoint, secret)
    assert len(created) == 1


def test_each_reader_gets_its_own_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake(monkeypatch)

    _, first = _core._endpoint("hdfs://a:8020", user=None, options=None)
    _, second = _core._endpoint("hdfs://b:8020", user=None, options=None)

    assert first != second, "one reader's credentials must not unlock another"
    assert {created[0].kwargs["auth_secret"], created[1].kwargs["auth_secret"]} == {
        first,
        second,
    }


def test_authorityless_uri_uses_environment_config_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("HADOOP_CONF_DIR", str(tmp_path))
    created = install_fake(monkeypatch)

    _core._endpoint(None, user=None, options=None)

    config = created[0].config
    assert config is not None
    assert 'namenode_uri = "hdfs://"' in config
    assert f"hdfs_config_dir = {_core._toml(str(tmp_path.resolve()))}" in config


def test_explicit_config_dir_wins_over_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    explicit = tmp_path / "explicit"
    explicit.mkdir()
    fallback = tmp_path / "fallback"
    fallback.mkdir()
    monkeypatch.setenv("HADOOP_CONF_DIR", str(fallback))
    created = install_fake(monkeypatch)

    _core._endpoint(None, user=None, options=None, config_dir=explicit)

    config = created[0].config
    assert config is not None
    assert f"hdfs_config_dir = {_core._toml(str(explicit.resolve()))}" in config
    assert str(fallback.resolve()) not in config


def test_default_fs_can_come_from_hdfs_options(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake(monkeypatch)

    _core._endpoint(
        None,
        user=None,
        options={"fs.defaultFS": "hdfs://nn:8020"},
    )

    config = created[0].config
    assert config is not None
    assert 'namenode_uri = "hdfs://"' in config
    assert 'hdfs_options = { "fs.defaultFS" = "hdfs://nn:8020" }' in config


def test_hdfs_user_options_and_config_dir_use_config_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    created = install_fake(monkeypatch)

    _core._endpoint(
        "hdfs://nn:8020", user="alice", options={"dfs.x": "y"}, config_dir=tmp_path
    )

    config = created[0].config
    assert config is not None
    assert 'namenode_uri = "hdfs://nn:8020"' in config
    assert 'hdfs_user = "alice"' in config
    assert f"hdfs_config_dir = {_core._toml(str(tmp_path.resolve()))}" in config
    assert 'hdfs_options = { "dfs.x" = "y" }' in config
    # The config-file path binds loopback and carries a secret too.
    assert 'listen_addr = "127.0.0.1:0"' in config
    assert any(line.startswith("auth_secret = ") for line in config.splitlines())


def test_config_dir_is_canonicalized(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    created = install_fake(monkeypatch)
    conf = tmp_path / "conf"
    conf.mkdir()

    first = _core._endpoint("hdfs://nn:8020", user=None, options=None, config_dir=conf)
    second = _core._endpoint(
        "hdfs://nn:8020", user=None, options=None, config_dir=f"{conf}/"
    )

    assert first == second
    assert len(created) == 1


def test_different_namenodes_get_separate_readers(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake(monkeypatch)

    _core._endpoint("hdfs://a:8020", user=None, options=None)
    _core._endpoint("hdfs://b:8020", user=None, options=None)

    assert len(created) == 2


def test_option_order_does_not_create_new_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake(monkeypatch)

    _core._endpoint("hdfs://nn:8020", user="alice", options={"a": "1", "b": "2"})
    _core._endpoint("hdfs://nn:8020", user="alice", options={"b": "2", "a": "1"})

    assert len(created) == 1


def test_shutdown_stops_gateways(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake(monkeypatch)
    _core._endpoint("hdfs://nn:8020", user=None, options=None)

    _core.shutdown()

    assert created[0].stopped


def test_translate_hdfs_uri() -> None:
    assert _core._translate("hdfs://nn:8020/a/b.parquet") == (
        "hdfs://nn:8020",
        "s3://hdfs/a/b.parquet",
    )
    assert _core._translate("hdfs:///a/b.parquet") == (None, "s3://hdfs/a/b.parquet")
    assert _core._translate("hdfs://nn:8020/a/b/*.parquet") == (
        "hdfs://nn:8020",
        "s3://hdfs/a/b/*.parquet",
    )


def test_translate_keeps_the_path_literal() -> None:
    # polars treats an s3:// key as literal text and percent-encodes it when it
    # builds the request, so translating must not touch it: encoding would apply
    # twice, and decoding would silently address a different object.
    assert _core._translate("hdfs://nn/t/year=2025/a b/é/") == (
        "hdfs://nn",
        "s3://hdfs/t/year=2025/a b/é/",
    )
    assert _core._translate("hdfs://nn/a%3Db.parquet") == (
        "hdfs://nn",
        "s3://hdfs/a%3Db.parquet",
    )


def test_non_hdfs_sources_are_ignored() -> None:
    assert _core._translate("local.parquet") is None
    assert _core._translate(b"hdfs://bytes") is None
    assert _core._translate(["a.parquet", "b.parquet"]) is None


def test_bad_hdfs_sources_raise() -> None:
    with pytest.raises(ValueError, match="query"):
        _core._translate("hdfs://nn:8020/a?x=1")
    with pytest.raises(ValueError, match="lists"):
        _core._translate(["hdfs://nn:8020/a", "hdfs://nn:8020/b"])


def _translate(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    *,
    keyword: bool = False,
    **kwargs: Any,
) -> tuple[tuple[Any, ...], dict[str, Any], list[Any]]:
    endpoint_calls: list[Any] = []

    def fake_endpoint(
        namenode: str | None,
        *,
        user: str | None,
        options: Any,
        config_dir: Any = None,
    ) -> tuple[str, str]:
        endpoint_calls.append((namenode, user, options, config_dir))
        return "http://127.0.0.1:4321", "test-secret"

    monkeypatch.setattr(_core, "_endpoint", fake_endpoint)
    if keyword:
        args, translated = _core._translate_call((), {"source": source, **kwargs})
    else:
        args, translated = _core._translate_call((source,), kwargs)
    return args, translated, endpoint_calls


def test_translate_call_injects_connection_options(monkeypatch: pytest.MonkeyPatch) -> None:
    args, kwargs, endpoint_calls = _translate(
        monkeypatch,
        "hdfs://nn:8020/data/a.parquet",
        columns=["x"],
        storage_options={
            "aws_region": "eu",
            "aws_endpoint_url": "http://user-supplied",
            "aws_skip_signature": "true",
            "aws_access_key_id": "user-key",
            "aws_secret_access_key": "user-secret",
        },
    )

    assert args == ("s3://hdfs/data/a.parquet",)
    assert kwargs == {
        "columns": ["x"],
        "storage_options": {
            "aws_region": "eu",
            "aws_access_key_id": "polars-hdfs",
            "aws_secret_access_key": "test-secret",
            "aws_allow_http": "true",
            "aws_endpoint_url": "http://127.0.0.1:4321",
        },
    }
    assert endpoint_calls == [("hdfs://nn:8020", None, None, None)]


def test_translate_call_handles_keyword_source(monkeypatch: pytest.MonkeyPatch) -> None:
    args, kwargs, endpoint_calls = _translate(
        monkeypatch,
        "hdfs:///data/a.parquet",
        keyword=True,
        storage_options={
            "hdfs_user": "alice",
            "hdfs_config_dir": "/conf",
            "hdfs_options": {"dfs.x": "y"},
        },
    )

    assert args == ()
    assert kwargs["source"] == "s3://hdfs/data/a.parquet"
    assert kwargs["storage_options"] == {
        "aws_access_key_id": "polars-hdfs",
        "aws_secret_access_key": "test-secret",
        "aws_allow_http": "true",
        "aws_endpoint_url": "http://127.0.0.1:4321",
    }
    assert endpoint_calls == [(None, "alice", {"dfs.x": "y"}, "/conf")]


def test_translate_call_leaves_non_hdfs_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    storage_options = {
        "hdfs_user": "alice",
        "hdfs_config_dir": "/conf",
        "hdfs_options": {"dfs.x": "y"},
        "aws_region": "eu",
    }
    args, kwargs, endpoint_calls = _translate(
        monkeypatch, "local.parquet", storage_options=storage_options
    )

    assert args == ("local.parquet",)
    assert kwargs["storage_options"] == storage_options
    assert endpoint_calls == []


def _fake_reader(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, list[Any]]:
    calls: list[Any] = []

    def fake(source: str, *, storage_options: Any = None, **kwargs: Any) -> str:
        calls.append((source, storage_options, kwargs))
        return "result"

    monkeypatch.setattr(pl, "read_parquet", fake)
    monkeypatch.setattr(
        _core, "_endpoint", lambda *args, **kwargs: ("http://127.0.0.1:4321", "s")
    )
    return fake, calls


def test_patch_hooks_alias_captured_before_register(monkeypatch: pytest.MonkeyPatch) -> None:
    fake, calls = _fake_reader(monkeypatch)
    early = pl.read_parquet
    _core.register()
    try:
        assert early(
            "hdfs://nn/data/a.parquet",
            columns=["x"],
            storage_options={"aws_region": "eu"},
        ) == "result"
        assert early("local.parquet", storage_options={"aws_region": "eu"}) == "result"
    finally:
        _core.unregister()

    assert calls == [
        (
            "s3://hdfs/data/a.parquet",
            {
                "aws_region": "eu",
                "aws_access_key_id": "polars-hdfs",
                "aws_secret_access_key": "s",
                "aws_allow_http": "true",
                "aws_endpoint_url": "http://127.0.0.1:4321",
            },
            {"columns": ["x"]},
        ),
        ("local.parquet", {"aws_region": "eu"}, {}),
    ]
    assert pl.read_parquet is fake


@pytest.mark.skipif(
    _ARGUMENT_REMOVED_ERROR is None,
    reason="polars.exceptions.ArgumentRemovedError was added in polars 2.0",
)
def test_patch_preserves_signature_and_removed_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed_error = _ARGUMENT_REMOVED_ERROR
    assert removed_error is not None
    fn = pl.read_parquet
    original_code = fn.__code__
    monkeypatch.setattr(
        _core, "_endpoint", lambda *args, **kwargs: ("http://127.0.0.1:4321", "s")
    )

    _core.register()
    try:
        assert fn.__code__ is not original_code
        assert "storage_options" in inspect.signature(fn).parameters
        with pytest.raises(removed_error, match="row_count_name"):
            fn("hdfs://nn/x.parquet", row_count_name="i")
    finally:
        _core.unregister()

    assert fn.__code__ is original_code


def test_register_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _core, "_endpoint", lambda *args, **kwargs: ("http://127.0.0.1:4321", "s")
    )
    _core.register()
    try:
        code = pl.read_parquet.__code__
        _core.register()
        assert pl.read_parquet.__code__ is code
    finally:
        _core.unregister()


def _raising_endpoint(*args: Any, **kwargs: Any) -> tuple[str, str]:
    raise RuntimeError("stop here")


def test_patched_frame_locals_are_readable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reading a patched frame's locals must not crash the interpreter.

    The generated hook once declared the original function's free variables
    without initialising them, so ``frame.f_locals`` -- used by debuggers,
    ``inspect.stack``, and pytest's own traceback formatting -- segfaulted.
    """
    seen: dict[str, Any] = {}

    def endpoint(*args: Any, **kwargs: Any) -> str:
        frame = inspect.currentframe()
        while frame is not None:
            if frame.f_code.co_filename == "<polars_hdfs>":
                seen["locals"] = frame.f_locals
                break
            frame = frame.f_back
        raise RuntimeError("stop here")

    monkeypatch.setattr(_core, "_endpoint", endpoint)
    _core.register()
    try:
        with pytest.raises(RuntimeError, match="stop here"):
            pl.read_parquet("hdfs://nn:8020/a.parquet")
    finally:
        _core.unregister()

    assert "locals" in seen, "the patched frame should be on the stack"
    assert set(seen["locals"]) >= {"args", "kwargs"}


def test_traceback_frames_are_introspectable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_core, "_endpoint", _raising_endpoint)
    _core.register()
    try:
        try:
            pl.read_parquet("hdfs://nn:8020/a.parquet")
        except RuntimeError:
            tb = sys.exc_info()[2]
    finally:
        _core.unregister()

    patched = []
    while tb is not None:
        assert tb.tb_frame.f_locals is not None  # used to segfault here
        patched.append(tb.tb_frame.f_code.co_filename)
        tb = tb.tb_next
    assert "<polars_hdfs>" in patched


def test_errors_from_the_original_function_propagate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Errors raised by the wrapped polars function reach the caller unchanged."""

    class Boom(Exception):
        pass

    def fake(source: str, *, storage_options: Any = None, **kwargs: Any) -> str:
        raise Boom("original failure")

    monkeypatch.setattr(pl, "read_parquet", fake)
    monkeypatch.setattr(
        _core, "_endpoint", lambda *args, **kwargs: ("http://127.0.0.1:4321", "s")
    )
    _core.register()
    try:
        with pytest.raises(Boom, match="original failure") as excinfo:
            pl.read_parquet("hdfs://nn:8020/a.parquet")
    finally:
        _core.unregister()

    names = [frame.name for frame in traceback.extract_tb(excinfo.tb)]
    assert "fake" in names, "the original function's frame must still be in the traceback"
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
