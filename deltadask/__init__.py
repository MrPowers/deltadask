import dask.dataframe as dd
from deltalake import DeltaTable

# columns: ["col1", "col2"]
# filters: [[('col1', '==', 0), ...], ...]
def read_delta(table_uri, version=None, storage_options=None, without_files=False, columns=None, filters=None):
    dt = DeltaTable(table_uri, version, storage_options, without_files)
    if filters:
        filenames = [table_uri + "/" + f for f in dt.files_by_partitions(filters)]
    else:
        filenames = [table_uri + "/" + f for f in dt.files()]
    ddf = dd.read_parquet(filenames, engine="pyarrow", columns=columns, filters=filters)
    return ddf
