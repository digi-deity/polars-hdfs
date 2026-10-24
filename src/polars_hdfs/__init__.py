"""Read ``hdfs://`` paths with polars.

Importing the package adds ``hdfs://`` support to polars' read/scan functions.
Calls with any other source are passed through unchanged.

    import polars as pl
    import polars_hdfs

    df = pl.read_parquet("hdfs:///data/events.parquet")
"""

from ._core import register, shutdown, unregister

__all__ = ["register", "shutdown", "unregister"]

register()
