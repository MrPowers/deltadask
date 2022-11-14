import dask.dataframe as dd
from deltalake import DeltaTable

# columns: ["col1", "col2"]
# filters: [[('col1', '==', 0), ...], ...]
def read_delta(path, columns=None, filters=None):
    dt = DeltaTable(path)
    if filters:
        filenames = [path + "/" + f for f in dt.files_by_partitions(filters)]
    else:
        filenames = [path + "/" + f for f in dt.files()]
    ddf = dd.read_parquet(filenames, engine="pyarrow", columns=columns, filters=filters)
    return ddf
