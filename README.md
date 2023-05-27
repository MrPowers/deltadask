# deltadask

**Important Update: Development on this project has shifted to the [dask-deltatable](https://github.com/dask-contrib/dask-deltatable) repo.**

A connector for reading Delta Lake tables into Dask DataFrames.

Install with `pip install deltadask`.

Read a Delta Lake into a Dask DataFrame as follows:

```python
import deltadask

ddf = deltadask.read_delta("path/to/delta/table")
```

It's a lot more efficient for Dask to read a Delta table compared to a Parquet data lake.  Parquet tables are of course faster than CSV tables [as explained in this video](https://www.youtube.com/watch?v=9LYYOdIwQXg).  Delta tables are the next big performance improvement for Dask users.

## Basic usage

Suppose you have a Delta table with the following three versions.

![Delta table with version](https://github.com/MrPowers/deltadask/blob/main/images/delta-table-with-versions.png)

Here's how to read the latest version of the Delta table:

```python
deltadask.read_delta("path/to/delta/table").compute()
```

```
   id
0   7
1   8
2   9
```

And here's how to read version 1 of the Delta table:

```python
deltadask.read_delta("path/to/delta/table", version=1).compute()
```

```
   id
0   0
1   1
2   2
0   4
1   5
```

Delta Lake makes it easy to time travel between different versions of a Delta table with Dask.

See [this notebook](https://github.com/MrPowers/deltadask/blob/main/notebooks/deltadask-demo.ipynb) for a full working example with an environment so you can replicate this on your machine.

## Why Delta Lake is better than Parquet for Dask

A Delta table stores data in Parquet files and metadata in a trasaction log.  The metadata includes the schema and location of the files.

![Delta table architecture](https://github.com/MrPowers/deltadask/blob/main/images/delta-table.png)

A Dask Parquet data lake can be stored in two different ways.

1. Parquet files with a single metadata file
2. Parquet files without a metadata file

Parquet files with a single metadata file is bad because of scaling limitations.

Parquet files without a metadata file require expensive file listing / Parquet footer queries to build the overall metadata statistics for the data lake.  It's a lot faster to fetch this information from the Delta transaction log.

Delta Lake is better because the transaction log is scalable and can be queried must faster than a data lake.

## How to make reads faster

You can make Delta Lake queries faster by using column projection and predicate pushdown filtering.  These tactics allow you to send less data to the cluster.

Here's an example of how to query a Delta table with Dask and take advantage of column pruning and predicate pushdown filtering:

```python
ddf = deltadask.read_delta(
    "path/to/delta/table", 
    columns=["col1"], # column pruning
    filters=[[('col1', '==', 0)]] # predicate pushdown
)
```

This query only sends `col1` to the cluster and none of the other columns (column projection).

This query also uses the transaction log to idenfity files that at least contain some data with `col1` equal to zero.  If a file contains no matching data, then it won't be read.  Depending on how the data is organized, a lot of files can be skipped.  You can skip the number of files even more by partitioning or Z ORDERING the data.

## How this library is built

The [delta-rs](https://github.com/delta-io/delta-rs/) library makes it really easy to build the deltadask connector.

All the transaction log parsing logic is handled by delta-rs.  You can just plug into the APIs to easily build the Dask connector.
