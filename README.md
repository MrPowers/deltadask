# deltadask

A connector for reading Delta Lake tables into Dask DataFrames.

Install with `pip install deltadask`.

Read a Delta Lake into a Dask DataFrame as follows:

```python
import deltadask

ddf = deltadask.read_delta(
    "path/to/delta/table", 
    columns=["col1"], filters=[[('col1', '==', 0)]])
```

## Why Delta Lake is better than Parquet for Dask

A Delta table stores data in Parquet files and metadata in a trasaction log.  The metadata includes the schema and location of the files.

![Delta table architecture](https://github.com/MrPowers/deltadask/blob/main/images/delta-table.png)

A Dask Parquet data lake can be stored in two different ways.

1. Parquet files with a single metadata file
2. Parquet files without a metadata file

Parquet files with a single metadata file are limited because a single file has scaling limitations.

Parquet files without a metadata file are limited because they require a relatively expensive file listing operation followed by calls to build the overall metadata statistics for the data lake.

Delta Lake is better because the transaction log is scalable and can be queried a lot faster than an expensive file listing operation.

## Why this library is really easy to build

Reading a Delta Lake into a Dask DataFrame is ridiculously easy, thanks to [delta-rs](https://github.com/delta-io/delta-rs/).

Reading Delta Lakes is also really fast and efficient.  You can get a list of the files from the transaction log which is a lot faster than a file listing operation.

You can also skip entire files based on column metadata stored in the transaction log.  Skipping data allows for huge performance improvements.

Here's how to read a Delta Lake into a Dask DataFrame with this library:
