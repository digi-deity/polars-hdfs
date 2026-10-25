"""Fixtures for integration tests against live HDFS clusters.

Opt in by pointing ``POLARS_HDFS_TEST_URI`` (and optionally
``POLARS_HDFS_TEST_URI_2``) at running namenodes; without them every test in this
directory is skipped.

``hdfs_native`` is used only to seed fixtures and clean up. It is the same client
the gateway uses internally, so seeding it that way keeps the tests focused on
what is unique to ``polars_hdfs`` rather than on re-testing HDFS itself.
"""

from __future__ import annotations

import os
import posixpath
import uuid
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import pytest

from polars_hdfs import _core

URI = os.environ.get("POLARS_HDFS_TEST_URI")
URI_2 = os.environ.get("POLARS_HDFS_TEST_URI_2")
BASE = os.environ.get("POLARS_HDFS_TEST_BASE", "/data")


@dataclass
class Cluster:
    """A live cluster plus an isolated, self-cleaning namespace on it."""

    uri: str
    root: str
    client: Any = field(repr=False)

    @classmethod
    def create(cls, uri: str) -> Cluster:
        hdfs_native = pytest.importorskip(
            "hdfs_native", reason="install the `integration` extra to seed fixtures"
        )
        client = hdfs_native.Client(uri)
        root = f"{BASE}/polars_hdfs_{uuid.uuid4().hex}"
        client.mkdirs(root, create_parent=True)
        return cls(uri=uri, root=root, client=client)

    def path(self, name: str = "") -> str:
        """Absolute HDFS path inside this cluster's namespace."""
        return posixpath.join(self.root, name) if name else self.root

    def uri_for(self, name: str = "") -> str:
        """``hdfs://`` URI for a path inside this cluster's namespace."""
        return f"{self.uri}{self.path(name)}"

    def write(self, name: str, data: bytes) -> str:
        """Write ``data`` to ``name`` and return its ``hdfs://`` URI."""
        path = self.path(name)
        parent = posixpath.dirname(path)
        if parent:
            self.client.mkdirs(parent, create_parent=True)
        with self.client.create(path) as writer:
            writer.write(data)
        return self.uri_for(name)

    def cleanup(self) -> None:
        with suppress(Exception):
            self.client.delete(self.root, recursive=True)


@pytest.fixture(autouse=True)
def _registered() -> Iterator[None]:
    # The unit-test conftest unregisters before every test, so register again:
    # these tests exercise the package the way a user gets it, hooks included.
    _core.register()
    yield
    _core.unregister()


@pytest.fixture
def cluster() -> Iterator[Cluster]:
    if not URI:
        pytest.skip("set POLARS_HDFS_TEST_URI to run integration tests")
    live = Cluster.create(URI)
    try:
        yield live
    finally:
        live.cleanup()


@pytest.fixture
def second_cluster() -> Iterator[Cluster]:
    if not URI_2:
        pytest.skip("set POLARS_HDFS_TEST_URI_2 to run multi-cluster tests")
    live = Cluster.create(URI_2)
    try:
        yield live
    finally:
        live.cleanup()
