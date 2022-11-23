import pytest
import beavis
import pandas as pd
import dask.dataframe as dd

import deltadask


def test_read_reference_table1():
    actual_ddf = deltadask.read_delta("./tests/reference_tables/generated/reference_table_1/delta")
    df = pd.DataFrame({
        "letter": ["a", "b", "c", "d", "e"],
         "number": [1, 2, 3, 4, 5],
          "a_float": [1.1, 2.2, 3.3, 4.4, 5.5]})
    expected_ddf = dd.from_pandas(df, npartitions=2)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)


def test_read_reference_table2():
    actual_ddf = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_2/delta", 
        filters=[("letter", "=", "a")])
    df = pd.DataFrame({
        "number": [4, 1],
        "a_float": [4.4, 1.1],
        "letter": ["a", "a"]})
    expected_ddf = dd.from_pandas(df, npartitions=2)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False, check_dtype=False)
