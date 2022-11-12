# deltadask

A connector for reading Delta Lake tables into Dask DataFrames.

Reading a Delta Lake into a Dask DataFrame is ridiculously easy, thanks to [delta-rs](https://github.com/delta-io/delta-rs/).

Reading Delta Lakes is also really fast and efficient.  You can get a list of the files from the transaction log which is a lot faster than a file listing operation.

You can also skip entire files based on column metadata stored in the transaction log.  Skipping data allows for huge performance improvements.

Here's how to read a Delta Lake into a Dask DataFrame with this library:

```python
import deltadask

ddf = deltadask.read_delta("path/to/delta/table", columns=["col1"], filters=[[('col1', '==', 0)]])
```
