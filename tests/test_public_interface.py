import pytest

import deltadask


def test_read_reference_table1():
    ddf = deltadask.read_delta("./tests/reference_tables/generated/reference_table_1/delta")
    print(ddf.compute())
