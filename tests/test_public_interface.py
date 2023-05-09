import pytest
import pandas as pd
import dask.dataframe as dd
import beavis

import deltadask


def test_reader_all_primitive_types():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/all_primitive_types/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/all_primitive_types/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


def test_reader_basic_append():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/basic_append/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/basic_append/expected/latest/table_content/*parquet"
    )
    beavis.assert_pd_equality(
        actual_ddf.compute().sort_values(by=["letter"]),
        expected_ddf.compute(),
        check_index=False,
    )

    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/basic_append/delta", version=0
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/basic_append/expected/v0/table_content/*parquet"
    )
    beavis.assert_pd_equality(
        actual_ddf.compute().sort_values(by=["letter"]),
        expected_ddf.compute(),
        check_index=False,
    )

    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/basic_append/delta", version=1
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/basic_append/expected/v1/table_content/*parquet"
    )
    beavis.assert_pd_equality(
        actual_ddf.compute().sort_values(by=["letter"]),
        expected_ddf.compute(),
        check_index=False,
    )


# def test_reader_basic_partitioned():
#     actual_ddf = deltadask.read_delta(
#         "./tests/reader_tests/generated/basic_partitioned/delta"
#     )
#     expected_ddf = dd.read_parquet(
#         "tests/reader_tests/generated/basic_partitioned/expected/latest/table_content/*parquet"
#     )
#     beavis.assert_pd_equality(
#         actual_ddf.compute().sort_values(by=["letter"]),
#         expected_ddf.compute(),
#         check_index=False,
#     )
#
#     actual_ddf = deltadask.read_delta(
#         "./tests/reader_tests/generated/basic_append/delta", version=0
#     )
#     expected_ddf = dd.read_parquet(
#         "tests/reader_tests/generated/basic_append/expected/v0/table_content/*parquet"
#     )
#     print(actual_ddf.compute())
#     beavis.assert_pd_equality(
#         actual_ddf.compute().sort_values(by=["letter"]),
#         expected_ddf.compute(),
#         check_index=False,
#     )

def test_reader_nested_types():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/nested_types/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/nested_types/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


def test_reader_no_replay():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/no_replay/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/no_replay/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


def test_reader_no_stats():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/no_stats/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/no_stats/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


def test_reader_stats_as_structs():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/stats_as_struct/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/stats_as_struct/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


def test_reader_with_checkpoint():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/with_checkpoint/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/with_checkpoint/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


def test_reader_with_schema_change():
    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/with_schema_change/delta"
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/with_schema_change/expected/latest/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())


    actual_ddf = deltadask.read_delta(
        "./tests/reader_tests/generated/with_schema_change/delta", version=1
    )
    expected_ddf = dd.read_parquet(
        "tests/reader_tests/generated/with_schema_change/expected/v1/table_content/*parquet"
    )
    pd.testing.assert_frame_equal(actual_ddf.compute(), expected_ddf.compute())
