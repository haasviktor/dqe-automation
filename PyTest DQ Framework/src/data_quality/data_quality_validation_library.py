import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    Checks Provided:
        - check_duplicates           : Uniqueness validation
        - check_count                : Completeness (row count) validation
        - check_data_full_data_set   : Full dataset comparison
        - check_dataset_is_not_empty : Consistency (non-empty) validation
        - check_not_null_values      : Validity (not-null) validation
        - check_no_negative_values   : Validity (non-negative) validation
    """

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names: list = None) -> None:
        """
        Check that no duplicate records exist in the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            column_names (list, optional): Columns to check for duplicates.
                                           If None, checks all columns.

        Raises:
            AssertionError: If duplicate records are found.
        """
        if column_names:
            duplicate_mask = df.duplicated(subset=column_names, keep=False)
        else:
            duplicate_mask = df.duplicated(keep=False)

        duplicates = df[duplicate_mask]

        assert duplicates.empty, (
            f"[check_duplicates] Duplicate records found.\n"
            f"  Columns checked : {column_names if column_names else 'All columns'}\n"
            f"  Total duplicates: {len(duplicates)}\n"
            f"  Duplicate rows  :\n{duplicates.to_string(index=True)}"
        )

    @staticmethod
    def check_count(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
        """
        Check that two DataFrames have the same number of rows.

        Args:
            df1 (pd.DataFrame): Source DataFrame.
            df2 (pd.DataFrame): Target DataFrame.

        Raises:
            AssertionError: If row counts differ.
        """
        count1 = len(df1)
        count2 = len(df2)

        assert count1 == count2, (
            f"[check_count] Row count mismatch between source and target.\n"
            f"  Source row count: {count1}\n"
            f"  Target row count: {count2}\n"
            f"  Difference      : {abs(count1 - count2)}"
        )

    @staticmethod
    def check_data_full_data_set(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
        """
        Perform a full dataset comparison between two DataFrames.

        Sorts both DataFrames, aligns common columns, and asserts
        that all values match between source and target.

        Args:
            df1 (pd.DataFrame): Source DataFrame.
            df2 (pd.DataFrame): Target DataFrame.

        Raises:
            AssertionError: If data mismatches are found.
        """
        common_columns = sorted(set(df1.columns) & set(df2.columns))

        if not common_columns:
            raise ValueError(
                "[check_data_full_data_set] No common columns found "
                "between source and target DataFrames."
            )

        df1_aligned = (
            df1[common_columns]
            .sort_values(by=common_columns)
            .reset_index(drop=True)
        )
        df2_aligned = (
            df2[common_columns]
            .sort_values(by=common_columns)
            .reset_index(drop=True)
        )

        try:
            mismatches = df1_aligned.compare(df2_aligned)
        except ValueError as e:
            raise AssertionError(
                f"[check_data_full_data_set] DataFrames could not be compared.\n"
                f"  Error: {e}"
            )

        assert mismatches.empty, (
            f"[check_data_full_data_set] Data mismatch found between source and target.\n"
            f"  Columns compared : {common_columns}\n"
            f"  Mismatched cells :\n{mismatches.to_string()}"
        )

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame) -> None:
        """
        Check that the DataFrame is not empty.

        Args:
            df (pd.DataFrame): The DataFrame to validate.

        Raises:
            AssertionError: If the DataFrame is empty.
        """
        assert not df.empty, (
            "[check_dataset_is_not_empty] Dataset is empty. "
            "Expected at least one row of data."
        )

    @staticmethod
    def check_not_null_values(
        df: pd.DataFrame, column_names: list = None
    ) -> None:
        """
        Check that specified columns (or all columns) contain no null values.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            column_names (list, optional): Columns to check for null values.
                                           If None, checks all columns.

        Raises:
            AssertionError: If null values are found in any column.
        """
        columns_to_check = column_names if column_names else df.columns.tolist()

        missing_columns = [c for c in columns_to_check if c not in df.columns]
        if missing_columns:
            raise ValueError(
                f"[check_not_null_values] Columns not found in DataFrame: "
                f"{missing_columns}"
            )

        null_report = {
            col: int(df[col].isnull().sum())
            for col in columns_to_check
            if df[col].isnull().sum() > 0
        }

        assert not null_report, (
            f"[check_not_null_values] Null values found in the following columns:\n"
            + "\n".join(
                f"  Column '{col}': {count} null value(s)"
                for col, count in null_report.items()
            )
        )
