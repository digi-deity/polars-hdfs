# polars-hdfs

Read HDFS paths directly with polars.

```python
import polars as pl
import polars_hdfs

df = pl.read_parquet("hdfs://namenode:8020/data/events.parquet")
lf = pl.scan_parquet("hdfs://namenode:8020/data/events/*.parquet")
```

Importing `polars_hdfs` adds `hdfs://` support to polars' read/scan functions.
Calls without an `hdfs://` source are passed through unchanged.

The readers are patched in place, so references captured before the import keep
working as long as `polars_hdfs` is imported before the read is executed:

```python
from polars import read_parquet
import polars_hdfs  # patched before the call

df = read_parquet("hdfs://namenode:8020/data/events.parquet")
```

## Install

```bash
pip install polars-hdfs
```

## Configuration

HDFS settings go in polars' `storage_options`, the same place S3, Azure, and GCP
settings go. Three keys are supported:

- `hdfs_config_dir`: a Hadoop configuration directory
- `hdfs_user`: the effective HDFS user
- `hdfs_options`: Hadoop configuration keys and values

### How configuration is resolved

When a read uses an `hdfs://` path, the HDFS client is configured like this:

1. Choose a Hadoop config directory. `hdfs_config_dir` wins if you set it;
   otherwise `HADOOP_CONF_DIR` is used, then `HADOOP_HOME/etc/hadoop`. If none
   is available, no XML is loaded.
2. Load `core-site.xml` and `hdfs-site.xml` from that directory, if present.
3. Apply `hdfs_options` on top. Since these are Hadoop keys, they override any
   matching values from XML.
4. Choose the NameNode. A URI authority like `hdfs://namenode:8020` is used
   directly. For a URI without an authority (`hdfs:///...`), the NameNode comes
   from `fs.defaultFS` in the resulting configuration.

If no NameNode can be found, the read fails with the HDFS client's missing
`fs.defaultFS` error.

`hdfs_user` is separate from `hdfs_options` because it is a client identity
setting, not a Hadoop configuration key. When set, it takes precedence over the
`HADOOP_USER_NAME` and `HADOOP_PROXY_USER` environment variables. When omitted,
those variables are used, followed by the operating-system user.

No Hadoop configuration is required for a direct NameNode. If a config
directory is available, it is still loaded for Kerberos, HA, datanode settings,
and the like; the URI authority only decides which NameNode to use.

## Examples

### Configuration directory

For authority-less URIs, the NameNode comes from `fs.defaultFS` in the config:

```python
pl.read_parquet(
    "hdfs:///data/file.parquet",
    storage_options={"hdfs_config_dir": "/etc/hadoop/conf"},
)
```

Use this for HA nameservices, Kerberos, and other cluster-specific settings.

### NameNode without a config file

`fs.defaultFS` can be supplied directly through `hdfs_options`:

```python
pl.read_parquet(
    "hdfs:///data/file.parquet",
    storage_options={
        "hdfs_options": {"fs.defaultFS": "hdfs://namenode:8020"},
    },
)
```

### Multiple clusters

Each distinct NameNode, user, Hadoop option set, and config directory gets its
own reader. Readers run in parallel, so datasets from different clusters can be
scanned lazily and joined in polars.

A single config directory can contain settings for multiple HA nameservices; the
URI authority selects the nameservice:

```python
so = {"hdfs_config_dir": "/etc/hadoop/conf"}

cluster_a = pl.scan_parquet("hdfs://cluster-a/data/events/", storage_options=so)
cluster_b = pl.scan_parquet("hdfs://cluster-b/data/users/", storage_options=so)

result = cluster_a.join(cluster_b, on="user_id").collect()
```

Different config directories can also provide different `fs.defaultFS` values:

```python
a = pl.scan_parquet(
    "hdfs:///data/events/",
    storage_options={"hdfs_config_dir": "/conf/cluster-a"},
)
b = pl.scan_parquet(
    "hdfs:///data/users/",
    storage_options={"hdfs_config_dir": "/conf/cluster-b"},
)
```

### HDFS options and user

```python
pl.scan_parquet(
    "hdfs:///data/events/",
    storage_options={
        "hdfs_user": "analytics",
        "hdfs_config_dir": "/etc/hadoop/conf",
        "hdfs_options": {
            "dfs.client.use.datanode.hostname": "true",
        },
    },
)
```

## Reader reuse and lifecycle

Reads with the same NameNode selector, user, Hadoop options, and config
directory reuse the same reader. Config directory paths are canonicalized, so
relative paths, trailing slashes, and equivalent paths share a reader. Different
config directories are kept separate even if their contents look similar. Use
the same NameNode style for datasets on the same cluster so they share a reader.

Readers start lazily, are reused for matching settings, and stay alive so lazy
frames can be collected later — a lazy frame may outlive the call that created
it, so there is no automatic eviction. The number of readers is bounded by the
number of distinct cluster configurations, not by the number of datasets. Call
`polars_hdfs.shutdown()` to stop them explicitly; they are also stopped at
interpreter exit.

## Advanced use

Most tools cannot read HDFS, but plenty of them can read S3. Each `hdfs://`
source is therefore served by an HDFS reader that presents itself as an S3
endpoint, and `credentials()` exposes that endpoint when you want another tool to
read the same data:

```python
creds = polars_hdfs.credentials("hdfs:///data/events/")
creds.s3_uri             # s3://hdfs/data/events/
creds.storage_options()  # endpoint and key pair
```

It accepts the same `hdfs_user`, `hdfs_options`, and `hdfs_config_dir` keywords
as `storage_options`.

## Supported calls

Parquet, CSV, IPC, NDJSON, lines, and Delta are supported, including directory
and glob sources where polars supports them.

A directory source needs a trailing slash. Without one the path is read as a
single key, the same way it is on S3:

```python
pl.scan_parquet("hdfs:///data/events/")           # every file in the directory
pl.scan_parquet("hdfs:///data/events/*.parquet")  # glob
pl.scan_parquet("hdfs:///data/events/a.parquet")  # one file
```

HDFS support is read-only. Writing, `read_avro`, and schema-only helpers are not
supported.

## Development

```bash
python -m pytest
```

That runs the unit tests, which need no cluster: the gateway is faked.

The tests in `tests/integration` read from real HDFS and are skipped unless
`POLARS_HDFS_TEST_URI` points at a running namenode. The Docker commands in
`.github/workflows/integration.yml` are the reference for starting one (or two)
locally:

```bash
POLARS_HDFS_TEST_URI=hdfs://localhost:9001 uv run pytest -m integration
```

`POLARS_HDFS_TEST_URI_2` adds the second cluster used by the multi-cluster join
test, and `POLARS_HDFS_TEST_BASE` (default `/data`) chooses the directory that
fixtures are written to and cleaned up from. Seeding uses `hdfs-native`, which is
what the `integration` extra installs:

```bash
uv sync --extra dev --extra integration
```
