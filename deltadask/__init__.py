import dask.dataframe as dd
from deltalake import DeltaTable


# columns: ["col1", "col2"]
# filters: [[('col1', '==', 0), ...], ...]
def read_delta(
    table_uri,
    version=None,
    storage_options=None,
    without_files=False,
    columns=None,
    filters=None,
):
    dt = DeltaTable(table_uri, version, storage_options, without_files)
    fragments = dt.to_pyarrow_dataset().get_fragments(filter=filters)
    filenames = list(f"{table_uri}/{fragment.path}" for fragment in fragments)
    ddf = dd.read_parquet(filenames, engine="pyarrow", columns=columns)
    return ddf

# import pyarrow as pa
# import pyarrow.dataset as ds
# from deltalake import DeltaTable
#
# dt = DeltaTable(f"{pathlib.Path.home()}/data/delta/G1_1e8_1e2_0_0")
# filter = ds.field('id1') == 'id016'
# fragments = dt.to_pyarrow_dataset().get_fragments(filter=filter)
# list(fragment.path for fragment in fragments)