from __future__ import annotations

import pytest

from polars_hdfs import _core


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch: pytest.MonkeyPatch) -> None:
    _core.unregister()
    _core.shutdown()
    for name in ("HADOOP_CONF_DIR", "HADOOP_HOME"):
        monkeypatch.delenv(name, raising=False)
    yield
    _core.unregister()
    _core.shutdown()
