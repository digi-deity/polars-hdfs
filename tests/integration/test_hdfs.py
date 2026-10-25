"""Integration tests against live HDFS clusters.

These cover what ``polars_hdfs`` adds on top of the gateway and hdfs-native, which
are tested in their own repositories: that the patched polars functions really
read ``hdfs://`` sources, that readers are reused per cluster configuration, that
one lazy query can span two clusters, and that readers outlive the call that
created them.
"""

from __future__ import annotations

import io
from typing import Any

import polars as pl
import pytest

from polars_hdfs import _core

pytestmark = pytest.mark.integration

DATA = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def encode(frame: pl.DataFrame, fmt: str) -> bytes:
    buffer = io.BytesIO()
    getattr(frame, f"write_{fmt}")(buffer)
    return buffer.getvalue()


@pytest.mark.parametrize(
    ("fmt", "read"),
    [
        ("parquet", pl.read_parquet),
        ("csv", pl.read_csv),
        ("ipc", pl.read_ipc),
        ("ndjson", pl.read_ndjson),
    ],
)
def test_each_patched_reader_reads_hdfs(cluster: Any, fmt: str, read: Any) -> None:
    uri = cluster.write(f"data.{fmt}", encode(DATA, fmt))
    assert read(uri).sort("id").equals(DATA)


def test_lines_reader_reads_hdfs(cluster: Any) -> None:
    uri = cluster.write("lines.txt", b"alpha\nbeta\n")
    assert pl.scan_lines(uri).collect().to_series(0).to_list() == ["alpha", "beta"]


def test_lazy_frame_collects_after_unrelated_reads(cluster: Any) -> None:
    lazy = pl.scan_parquet(cluster.write("lazy.parquet", encode(DATA, "parquet")))
    # The reader behind `lazy` must survive an unrelated read, since the lazy
    # frame outlives the call that created it.
    pl.read_parquet(cluster.write("other.parquet", encode(DATA, "parquet")))
    assert lazy.sort("id").collect().equals(DATA)


def test_reader_reuse_is_per_configuration(cluster: Any) -> None:
    first = cluster.write("a.parquet", encode(DATA, "parquet"))
    second = cluster.write("b.parquet", encode(DATA, "parquet"))

    pl.read_parquet(first)
    pl.read_parquet(second)
    assert len(_core._readers) == 1, "same cluster configuration should share a reader"

    pl.read_parquet(first, storage_options={"hdfs_user": "integration-other"})
    assert len(_core._readers) == 2, "a different user is a different reader"


def test_one_query_joins_two_clusters(cluster: Any, second_cluster: Any) -> None:
    users = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    events = pl.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
    left = pl.scan_parquet(cluster.write("users.parquet", encode(users, "parquet")))
    right = pl.scan_parquet(
        second_cluster.write("events.parquet", encode(events, "parquet"))
    )

    result = left.join(right, on="id").sort("id").collect()

    assert result["score"].to_list() == [10, 20, 30]
    assert len(_core._readers) == 2, "one reader per cluster"


def test_directory_and_glob_sources(cluster: Any) -> None:
    for year in (2021, 2022):
        cluster.write(f"table/year={year}/data.parquet", encode(DATA, "parquet"))

    # A directory source needs its trailing slash: without one the object store
    # treats the path as a key and the read fails with "not found".
    directory = pl.read_parquet(cluster.uri_for("table/"))
    globbed = pl.read_parquet(cluster.uri_for("table/*/*.parquet"))

    assert directory.height == 2 * DATA.height
    assert globbed.height == 2 * DATA.height


def test_shutdown_allows_a_fresh_read(cluster: Any) -> None:
    uri = cluster.write("again.parquet", encode(DATA, "parquet"))
    assert pl.read_parquet(uri).equals(DATA)

    _core.shutdown()
    assert _core._readers == {}

    assert pl.read_parquet(uri).equals(DATA)
