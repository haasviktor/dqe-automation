"""
Description: Data Quality checks for the patient_sum_treatment_cost_per_facility_type dataset.
             Validates that Parquet target data matches the PostgreSQL source data
             in terms of completeness, uniqueness, and validity.

             Parquet partitioned by: facility_type

             Transformations:
               - facility_type       : direct from facilities.facility_type       (grouping, not null)
               - full_name           : first_name || ' ' || last_name             (grouping, not null)
                                       format: <first_name> <last_name>
               - sum_treatment_cost  : SUM(treatment_cost)                        (not null,
                                       cannot be negative)

Requirement(s): TICKET-1236
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

    Joins visits → facilities and patients, groups by facility_type and full_name,
    and computes the total treatment cost per patient per facility type.

    Full name is constructed as: first_name + ' ' + last_name

    Args:
        db_connection: Session-scoped PostgreSQL connector fixture.

    Returns:
        pd.DataFrame: Aggregated source data from PostgreSQL.
    """
    source_query = """
        SELECT
            f.facility_type,
            p.first_name || ' ' || p.last_name  AS full_name,
            SUM(v.treatment_cost)               AS sum_treatment_cost
        FROM
            visits v
            JOIN facilities f ON v.facility_id = f.id
            JOIN patients   p ON v.patient_id  = p.id
        WHERE
            f.facility_type IS NOT NULL
            AND p.first_name IS NOT NULL
            AND p.last_name IS NOT NULL
            AND v.treatment_cost IS NOT NULL
            AND v.treatment_cost >= 0
        GROUP BY
            f.facility_type,
            p.first_name || ' ' || p.last_name
        ORDER BY
            f.facility_type,
            full_name
    """
    return db_connection.get_data_sql(source_query)


@pytest.fixture(scope="module")
def target_data(parquet_reader):
    """
    Load target data from Parquet files.

    Reads all Parquet files including subfolders to cover
    all facility_type partitions.

    Args:
        parquet_reader: Session-scoped ParquetReader fixture.

    Returns:
        pd.DataFrame: Target data loaded from Parquet files.
    """
    target_path = "/parquet_data/patient_sum_treatment_cost_per_facility_type"
    return parquet_reader.process(target_path, include_subfolders=True)


# ---------------------------------------------------------------------------
# Smoke Tests
# ---------------------------------------------------------------------------

@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    """
    Smoke test: Verify the target dataset is not empty.

    Ensures Parquet files across all facility_type partitions were
    successfully loaded and contain at least one row of data.
    """
    data_quality_library.check_dataset_is_not_empty(target_data)


# ---------------------------------------------------------------------------
# Data Completeness Tests
# ---------------------------------------------------------------------------

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_count(source_data, target_data, data_quality_library):
    """
    Completeness test: Verify row counts match between source and target.

    Ensures no records were dropped or duplicated during the aggregation
    pipeline (group by facility_type + full_name).
    """
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_data_completeness(source_data, target_data, data_quality_library):
    """
    Completeness test: Verify full dataset equality between source and target.

    Performs a row-by-row comparison across facility_type, full_name,
    and sum_treatment_cost columns.
    """
    data_quality_library.check_data_full_data_set(source_data, target_data)


# ---------------------------------------------------------------------------
# Data Quality Tests
# ---------------------------------------------------------------------------

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_uniqueness(target_data, data_quality_library):
    """
    Uniqueness test: Verify no duplicate records exist in the target dataset.

    Checks the combination of facility_type + full_name for uniqueness,
    as each patient should have exactly one sum_treatment_cost entry
    per facility type.
    """
    data_quality_library.check_duplicates(
        target_data,
        column_names=["facility_type", "full_name"]
    )


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_not_null_values(target_data, data_quality_library):
    """
    Validity test: Verify all columns contain no null values.

    All three columns are mandatory:
      - facility_type      : grouping key (partition), must not be null
      - full_name          : grouping key (first_name + last_name), must not be null
      - sum_treatment_cost : computed financial metric, must not be null
    """
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_type", "full_name", "sum_treatment_cost"]
    )
