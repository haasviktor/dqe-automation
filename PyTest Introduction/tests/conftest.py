import os
import pandas as pd
import pytest

@pytest.fixture(scope="session")
def read_csv_file(path_to_file):
    df = pd.read_csv(path_to_file)
    return df


@pytest.fixture(scope="session")
def validate_schema():
    def _validate(actual_schema, expected_schema):
        assert actual_schema == expected_schema, "FAIL – Schema mismatch."
    return _validate


# Hooks
def pytest_collection_modifyitems(items: list) -> None:
    for item in items:
        if not item.own_markers:
            item.add_marker(pytest.mark.unmarked)
