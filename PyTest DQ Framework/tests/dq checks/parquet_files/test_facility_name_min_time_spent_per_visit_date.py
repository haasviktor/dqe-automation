"""
Description: Data Quality checks for the facility_name_min_time_spent_per_visit_date dataset.
             Validates that Parquet target data matches the PostgreSQL source data
             in terms of completeness, uniqueness, and validity.

             Parquet partitioned by: visit_date (year-month, day excluded from partition key)

             Transformations:
               - facility_name  : direct from facilities.facility_name       (grouping, not null)
               - visit_date     : CAST(visit_timestamp AS DATE)               (grouping, not null)
               - min_time_spent : MIN(duration_minutes)                       (not null)

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
    Fetch source data from PostgreSQL.

    Joins facilities → visits, groups by facility_name and visit_date,
    and computes the minimum duration_minutes per group.

    Args:
        db_connection: Session-scoped PostgreSQL connector fixture.

    Returns:
        pd.DataFrame: Aggregated source data from PostgreSQL.
    """
    source_query = """
        SELECT
            f.facility_name,
            CAST(v.visit_timestamp AS DATE)  AS visit_date,
            MIN(v.duration_minutes)          AS min_time_spent
        FROM
            visits v
            JOIN facilities f ON v.facility_id = f.id
        GROUP BY
            f.facility_name,
            CAST(v.visit_timestamp AS DATE)
        ORDER BY
            f.facility_name,
            visit_date
    """
    return db_connection.get_data_sql(source_query)


@pytest.fixture(scope="module")
def target_data(parquet_reader):
    """
    Load target data from Parquet files.

    Reads all Parquet files including subfolders to cover
    all visit_date (year-month) partitions.

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

    Ensures Parquet files across all year-month partitions were
    successfully loaded and contain at least one row of data.
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

    Ensures no records were dropped or duplicated during the aggregation
    pipeline (group by facility_name + visit_date).
    """
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_completeness(source_data, target_data, data_quality_library):
    """
    Completeness test: Verify full dataset equality between source and target.

    Performs a row-by-row comparison across facility_name, visit_date,
    and min_time_spent columns.
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

    Checks the combination of facility_name + visit_date for uniqueness,
    as each facility should have exactly one min_time_spent entry per visit date.
    """
    data_quality_library.check_duplicates(
        target_data,
        column_names=["facility_name", "visit_date"]
    )


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    """
    Validity test: Verify all columns contain no null values.

    All three columns are mandatory:
      - facility_name  : grouping key, must not be null
      - visit_date     : grouping key, must not be null
      - min_time_spent : computed metric, must not be null
    """
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_name", "visit_date", "min_time_spent"]
    )