import pytest
import beavis
import pandas as pd
import dask.dataframe as dd

import deltadask


def test_read_reference_table1():
    actual_ddf0 = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_1/delta",
        version=0
    )
    expected_ddf0 = dd.read_parquet("tests/reference_tables/generated/reference_table_1/expected/v0/table_content.parquet")
    beavis.assert_dd_equality(actual_ddf0, expected_ddf0, check_index=False, check_dtype=False)

    actual_ddf1 = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_1/delta",
        version=1
    )
    expected_ddf1 = dd.read_parquet("tests/reference_tables/generated/reference_table_1/expected/v1/table_content.parquet")
    beavis.assert_dd_equality(actual_ddf1, expected_ddf1, check_index=False, check_dtype=False)

    actual_ddf = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_1/delta"
    )
    expected_ddf = dd.read_parquet("tests/reference_tables/generated/reference_table_1/expected/latest/table_content.parquet")
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False, check_dtype=False)


def test_read_reference_table2():
    # TODO: Add this test to DAT
    actual_ddf = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_2/delta", 
        filters=[("letter", "=", "a")])
    df = pd.DataFrame({
        "number": [4, 1],
        "a_float": [4.4, 1.1],
        "letter": ["a", "a"]})
    expected_ddf = dd.from_pandas(df, npartitions=2)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False, check_dtype=False)

    actual_ddf0 = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_2/delta",
        version=0
    )[["letter", "number", "a_float"]]
    expected_ddf0 = dd.read_parquet("tests/reference_tables/generated/reference_table_2/expected/v0/table_content.parquet").sort_values("letter")
    beavis.assert_dd_equality(actual_ddf0, expected_ddf0, check_index=False, check_dtype=False)

    actual_ddf1 = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_2/delta",
        version=1
    )[["letter", "number", "a_float"]].sort_values("a_float")
    expected_ddf1 = dd.read_parquet("tests/reference_tables/generated/reference_table_2/expected/v1/table_content.parquet").sort_values("a_float")
    beavis.assert_dd_equality(actual_ddf1, expected_ddf1, check_index=False, check_dtype=False)

    actual_ddf = deltadask.read_delta(
        "./tests/reference_tables/generated/reference_table_2/delta"
    )[["letter", "number", "a_float"]].sort_values("a_float")
    expected_ddf = dd.read_parquet("tests/reference_tables/generated/reference_table_2/expected/latest/table_content.parquet").sort_values("a_float")
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False, check_dtype=False)
