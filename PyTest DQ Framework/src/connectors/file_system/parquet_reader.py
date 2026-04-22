import os
import pandas as pd


class ParquetReader:
    """
    Reads and processes Parquet files from a local or networked file system.

    Supports reading a single Parquet file, all Parquet files in a directory,
    or recursively reading all Parquet files across subdirectories.

    Usage:
        reader = ParquetReader()
        df = reader.process('/data/parquet_files', include_subfolders=True)
    """

    def process(self, path: str, include_subfolders: bool = True) -> pd.DataFrame:
        """
        Read and concatenate Parquet files from the given path.

        Args:
            path (str): Path to a Parquet file or a directory containing Parquet files.
            include_subfolders (bool): If True, recursively searches subdirectories.

        Returns:
            pd.DataFrame: A concatenated DataFrame of all Parquet files found.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            RuntimeError: If reading or processing Parquet files fails.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[ParquetReader] Path does not exist: '{path}'"
            )

        try:
            parquet_files = self._collect_parquet_files(path, include_subfolders)

            if not parquet_files:
                print(f"[ParquetReader] No Parquet files found at: '{path}'")
                return pd.DataFrame()

            dataframes = []
            for file_path in parquet_files:
                #print(f"[ParquetReader] Reading file: {file_path}")
                dataframes.append(pd.read_parquet(file_path))

            combined_df = pd.concat(dataframes, ignore_index=True)
            print(
                f"[ParquetReader] Successfully loaded {len(parquet_files)} file(s). "
                f"Total rows: {len(combined_df)}"
            )
            return combined_df

        except Exception as e:
            raise RuntimeError(
                f"[ParquetReader] Failed to read Parquet files from '{path}'.\n"
                f"Error: {e}"
            )

    @staticmethod
    def _collect_parquet_files(path: str, include_subfolders: bool) -> list:
        """
        Collect all Parquet file paths from the given directory.

        Args:
            path (str): Root path to search.
            include_subfolders (bool): Whether to search subdirectories recursively.

        Returns:
            list: A list of absolute paths to Parquet files.
        """
        # Single file provided directly
        if os.path.isfile(path):
            if path.endswith(".parquet"):
                return [path]
            return []

        parquet_files = []

        if include_subfolders:
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith(".parquet"):
                        parquet_files.append(os.path.join(root, file))
        else:
            for file in os.listdir(path):
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(path, file))

        return sorted(parquet_files)
