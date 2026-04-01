import pandas as pd
import pytest

expected_schema = ["id", "name", "age", "email", "is_active"]
filepath = "../src/data/data.csv"
_EMAIL_PATTERN = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

@pytest.fixture(scope="session")
def path_to_file():
    return filepath

def test_file_not_empty(read_csv_file):
    assert not read_csv_file.empty, "FAIL – CSV file is empty: DataFrame contains no rows."


@pytest.mark.validate_csv
def test_schema_validation(read_csv_file, validate_schema):
    validate_schema(read_csv_file.columns.tolist(), expected_schema)


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Skipped, because just asked")
def test_age_valid_values(read_csv_file):
    invalid_ages = read_csv_file["age"][~read_csv_file["age"].between(0, 100)].tolist()
    assert not invalid_ages, f"FAIL – invalid age values: {invalid_ages}"


@pytest.mark.validate_csv
def test_email_format(read_csv_file):
    invalid_emails = read_csv_file["email"][~read_csv_file["email"].str.match(_EMAIL_PATTERN, na=False)].tolist()
    assert not invalid_emails, f"FAIL – invalid e-mails: {invalid_emails}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Known duplicated rows.")
def test_no_duplicate_rows(read_csv_file: pd.DataFrame):
    duplicate_rows = read_csv_file[read_csv_file.duplicated(keep="first")]
    assert duplicate_rows.empty, f"FAIL – duplicate rows: {duplicate_rows.to_string(index=False)}"


@pytest.mark.parametrize("id, is_active", [
        (1, False),
        (2, True),
    ],
)
def test_is_active_parametrized(read_csv_file, id, is_active):
    row = read_csv_file[read_csv_file["id"] == id]
    assert row["is_active"].values[0] == is_active, f"FAIL – is_active mismatch for id={id}"


def test_is_active_id2_no_parametrize(read_csv_file):
    target_id = 2
    row = read_csv_file[read_csv_file["id"] == target_id]
    assert row["is_active"].values[0] is True, f"FAIL – is_active mismatch for id={target_id}"
