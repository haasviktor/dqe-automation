"""
Description: Data Quality checks for the facility_name_min_time_spent_per_visit_date dataset.
             Validates that Parquet target data matches the PostgreSQL source data
             in terms of completeness, uniqueness, and validity.
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest


# ---------------------------------------------------------------------------
# Module-Scoped Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def source_data(db_connection):
    """
    Fetch the source dataset from PostgreSQL.

    Retrieves the minimum time spent per visit date grouped by facility name
    directly from the database.

    Args:
        db_connection: Session-scoped PostgreSQL connector fixture.

    Returns:
        pd.DataFrame: Source data from PostgreSQL.
    """
    source_query = """
        SELECT
            facility_name,
            visit_date,
            MIN(time_spent) AS min_time_spent
        FROM
            facility_visits
        GROUP BY
            facility_name,
            visit_date
        ORDER BY
            facility_name,
            visit_date
    """
    return db_connection.get_data_sql(source_query)


@pytest.fixture(scope="module")
def target_data(parquet_reader):
    """
    Load the target dataset from Parquet files.

    Reads all Parquet files (including subfolders) from the designated
    storage path for this dataset.

    Args:
        parquet_reader: Session-scoped ParquetReader fixture.

    Returns:
        pd.DataFrame: Target data loaded from Parquet files.
    """
    target_path = "/parquet_data/facility_name_min_time_spent_per_visit_date"
    return parquet_reader.process(target_path, include_subfolders=True)


# ---------------------------------------------------------------------------
# Smoke Tests
# ---------------------------------------------------------------------------

@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    """
    Smoke test: Verify the target dataset is not empty.

    Ensures that Parquet files were successfully loaded and contain data
    before proceeding with more detailed validation checks.
    """
    data_quality_library.check_dataset_is_not_empty(target_data)


# ---------------------------------------------------------------------------
# Data Completeness Tests
# ---------------------------------------------------------------------------

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    """
    Completeness test: Verify row counts match between source and target.

    Ensures that no records were lost or duplicated during the data
    pipeline processing.
    """
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_completeness(source_data, target_data, data_quality_library):
    """
    Completeness test: Verify full dataset equality between source and target.

    Performs a row-by-row comparison of all shared columns between the
    PostgreSQL source data and the Parquet target data.
    """
    data_quality_library.check_data_full_data_set(source_data, target_data)


# ---------------------------------------------------------------------------
# Data Quality Tests
# ---------------------------------------------------------------------------

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_uniqueness(target_data, data_quality_library):
    """
    Uniqueness test: Verify no duplicate records exist in the target dataset.

    Checks the combination of facility_name and visit_date for uniqueness,
    as each facility should have exactly one entry per visit date.
    """
    data_quality_library.check_duplicates(
        target_data, column_names=["facility_name", "visit_date"]
    )


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    """
    Validity test: Verify critical columns contain no null values.

    Ensures that all key fields required for downstream consumption
    are populated in the target dataset.
    """
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_name", "visit_date", "min_time_spent"],
    )
