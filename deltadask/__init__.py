import dask.dataframe as dd
from deltalake import DeltaTable

def read_delta(path, columns=None, filters=None):
    dt = DeltaTable(path)
    filenames = [path + "/" + f for f in dt.files()]
    ddf = dd.read_parquet(filenames, engine="pyarrow", columns=columns, filters=filters)
    return ddf
