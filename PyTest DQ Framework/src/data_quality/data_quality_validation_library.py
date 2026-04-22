import pandas as pd

# Max rows to display in assertion messages to avoid overwhelming output
MAX_DISPLAY_ROWS = 10


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    Checks Provided:
        - check_duplicates           : Uniqueness validation
        - check_count                : Completeness (row count) validation
        - check_data_full_data_set   : Full dataset comparison
        - check_dataset_is_not_empty : Consistency (non-empty) validation
        - check_not_null_values      : Validity (not-null) validation
    """

    @staticmethod
    def _align_column_dtypes(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        columns: list
    ) -> tuple:
        """
        Normalize dtypes of specified columns between two DataFrames
        so they can be safely merged and compared.

        Conversion priority:
          1. datetime-like  → both cast to datetime64[ns]
          2. numeric        → both cast to float64
          3. fallback       → both cast to str

        Args:
            df1 (pd.DataFrame): First DataFrame (source).
            df2 (pd.DataFrame): Second DataFrame (target).
            columns (list): Columns to check and align.

        Returns:
            tuple:
                df1_normalized (pd.DataFrame)
                df2_normalized (pd.DataFrame)
                dtype_mismatch_report (dict): {col: (df1_dtype, df2_dtype)}
        """
        df1 = df1.copy()
        df2 = df2.copy()
        dtype_mismatch_report = {}

        for col in columns:
            dtype1 = df1[col].dtype
            dtype2 = df2[col].dtype

            # Same dtype — nothing to do
            if dtype1 == dtype2:
                continue

            dtype_mismatch_report[col] = (str(dtype1), str(dtype2))

            is_datetime1 = pd.api.types.is_datetime64_any_dtype(dtype1)
            is_datetime2 = pd.api.types.is_datetime64_any_dtype(dtype2)
            is_numeric1  = pd.api.types.is_numeric_dtype(dtype1)
            is_numeric2  = pd.api.types.is_numeric_dtype(dtype2)

            # Priority 1: datetime-like → datetime64[ns]
            if is_datetime1 or is_datetime2:
                try:
                    df1[col] = pd.to_datetime(df1[col])
                    df2[col] = pd.to_datetime(df2[col])
                    continue
                except Exception:
                    pass

            # Priority 2: numeric → float64
            if is_numeric1 or is_numeric2:
                try:
                    df1[col] = pd.to_numeric(df1[col], errors="raise")
                    df2[col] = pd.to_numeric(df2[col], errors="raise")
                    continue
                except Exception:
                    pass

            # Priority 3: fallback → str
            df1[col] = df1[col].astype(str)
            df2[col] = df2[col].astype(str)

        return df1, df2, dtype_mismatch_report

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
            f"  Duplicate rows (first {MAX_DISPLAY_ROWS}):\n"
            f"{duplicates.head(MAX_DISPLAY_ROWS).to_string(index=True)}"
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

        Processing steps:
          0. Dtype alignment  : normalize mismatched column dtypes
                                (e.g. object vs datetime64, int vs float)
          1. Row count check  : if counts differ, use outer merge to identify
                                exactly which rows are missing or extra
          2. Value comparison : if counts match, perform cell-by-cell comparison
                                using pandas.compare()

        Args:
            df1 (pd.DataFrame): Source DataFrame.
            df2 (pd.DataFrame): Target DataFrame.

        Raises:
            ValueError: If no common columns exist between the DataFrames.
            AssertionError: If row counts differ or cell values do not match.
        """
        # --- Validate common columns -------------------------------------------
        common_columns = sorted(set(df1.columns) & set(df2.columns))

        if not common_columns:
            raise ValueError(
                "[check_data_full_data_set] No common columns found "
                "between source and target DataFrames."
            )

        # --- Step 0: Normalize dtypes ------------------------------------------
        # Must happen BEFORE sort — mismatched types break sort_values too
        df1_normalized, df2_normalized, dtype_mismatches = (
            DataQualityLibrary._align_column_dtypes(
                df1[common_columns],
                df2[common_columns],
                common_columns
            )
        )

        if dtype_mismatches:
            print(
                f"\n[check_data_full_data_set] WARNING: dtype mismatches detected "
                f"and normalized before comparison:\n"
                + "\n".join(
                    f"  Column '{col}': "
                    f"source={src_dtype} → target={tgt_dtype} → normalized"
                    for col, (src_dtype, tgt_dtype) in dtype_mismatches.items()
                )
            )

        # --- Align: sort + reset index -----------------------------------------
        df1_aligned = (
            df1_normalized
            .sort_values(by=common_columns)
            .reset_index(drop=True)
        )
        df2_aligned = (
            df2_normalized
            .sort_values(by=common_columns)
            .reset_index(drop=True)
        )

        count_df1 = len(df1_aligned)
        count_df2 = len(df2_aligned)

        # --- Step 1: Row count check -------------------------------------------
        if count_df1 != count_df2:

            merged = df1_aligned.merge(
                df2_aligned,
                on=common_columns,
                how="outer",
                indicator=True
            )

            only_in_source = (
                merged[merged["_merge"] == "left_only"]
                .drop(columns=["_merge"])
                .reset_index(drop=True)
            )
            only_in_target = (
                merged[merged["_merge"] == "right_only"]
                .drop(columns=["_merge"])
                .reset_index(drop=True)
            )

            source_display = (
                f"{only_in_source.head(MAX_DISPLAY_ROWS).to_string(index=False)}\n"
                f"  ... and {len(only_in_source) - MAX_DISPLAY_ROWS} more rows"
                if len(only_in_source) > MAX_DISPLAY_ROWS
                else only_in_source.to_string(index=False)
            )
            target_display = (
                f"{only_in_target.head(MAX_DISPLAY_ROWS).to_string(index=False)}\n"
                f"  ... and {len(only_in_target) - MAX_DISPLAY_ROWS} more rows"
                if len(only_in_target) > MAX_DISPLAY_ROWS
                else only_in_target.to_string(index=False)
            )

            assert False, (
                f"[check_data_full_data_set] Row count mismatch "
                f"between source and target.\n\n"
                f"  Source row count : {count_df1}\n"
                f"  Target row count : {count_df2}\n"
                f"  Difference       : {abs(count_df1 - count_df2)}\n\n"
                f"  Rows only in source [{len(only_in_source)} row(s)]:\n"
                f"{source_display}\n\n"
                f"  Rows only in target [{len(only_in_target)} row(s)]:\n"
                f"{target_display}"
            )

        # --- Step 2: Cell value comparison ------------------------------------
        # Safe to call compare() — both DataFrames now have:
        #   - identical dtypes   (normalized in Step 0)
        #   - identical columns  (common_columns)
        #   - identical row count (validated in Step 1)
        #   - identical index    (0...n-1 from reset_index)
        try:
            mismatches = df1_aligned.compare(df2_aligned)
        except ValueError as e:
            raise AssertionError(
                f"[check_data_full_data_set] DataFrames could not be compared.\n"
                f"  Error: {e}"
            )

        mismatch_display = (
            f"{mismatches.head(MAX_DISPLAY_ROWS).to_string()}\n"
            f"  ... and {len(mismatches) - MAX_DISPLAY_ROWS} more rows"
            if len(mismatches) > MAX_DISPLAY_ROWS
            else mismatches.to_string()
        )

        assert mismatches.empty, (
            f"[check_data_full_data_set] Cell value mismatch "
            f"between source and target.\n\n"
            f"  Columns compared     : {common_columns}\n"
            f"  Total mismatched rows: {len(mismatches)}\n\n"
            f"  Mismatched cells (self=source, other=target):\n"
            f"{mismatch_display}"
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
