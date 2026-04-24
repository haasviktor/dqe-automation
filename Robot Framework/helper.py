import pandas as pd
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from robot.libraries.BuiltIn import BuiltIn 


def _get_driver():
    """Helper to retrieve Selenium WebDriver from SeleniumLibrary"""
    selenium_lib = BuiltIn().get_library_instance('SeleniumLibrary')
    return selenium_lib.driver


def read_html_table(filter_date=None):  
    """Extract table data into Pandas DataFrame"""

    driver = _get_driver()

    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "table"))
    )

    table = driver.find_element(By.CLASS_NAME, "table")
    columns = table.find_elements(By.XPATH, ".//*[@class='y-column']")

    headers = []
    column_data = []

    for col in columns:
        header = col.find_element(By.ID, "header").text.strip()
        headers.append(header)

        cells = col.find_elements(By.CSS_SELECTOR, ".cell-text")

        values = []
        for cell in cells:
            text = cell.text.strip()
            if text and text != header:
                values.append(text)

        column_data.append(values)

    rows = list(zip(*column_data))
    df = pd.DataFrame(rows, columns=headers)

    if filter_date and "Visit Date" in df.columns:
        df = df[df["Visit Date"] == filter_date]

    return df

def read_parquet_dataset(folder_path, filter_date=None):
    """
    Read partitioned parquet and filter by:
    - partition folder (YYYY-MM)
    - visit_date column (YYYY-MM-DD)
    """

    all_files = []

    partition_filter = None
    if filter_date:
        partition_filter = filter_date[:7]  # YYYY-MM

    for root, _, files in os.walk(folder_path):
        for f in files:
            if f.endswith(".parquet"):

                # 1. Filter by partition folder (YYYY-MM)
                if partition_filter:
                    if f"partition_date={partition_filter}" not in root:
                        continue

                full_path = os.path.join(root, f)
                all_files.append(full_path)

    if not all_files:
        raise Exception("No parquet files found for given partition!")

    df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

    # 2. Filter by visit_date column (YYYY-MM-DD)
    if filter_date:
        if "visit_date" not in df.columns:
            raise Exception("visit_date column not found in parquet!")

        df["visit_date"] = pd.to_datetime(df["visit_date"]).dt.strftime("%Y-%m-%d")
        df["visit_date"] = df["visit_date"].astype(str)
        df = df[df["visit_date"] == filter_date]

    return df

def normalize_columns(df):
    return df.rename(columns=lambda c: c.strip().lower().replace(" ", "_"))

COLUMN_MAPPING = {
    "average_time_spent": "avg_time_spent"
}

def apply_column_mapping(df):
    return df.rename(columns=COLUMN_MAPPING)

def normalize_numeric(df):
    """Convert numeric-like columns to float for consistent comparison"""
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass
    return df

def compare_dataframes(df1, df2):
    """Compare two DataFrames strictly"""

    # normalize column names
    df1 = normalize_columns(df1)
    df2 = normalize_columns(df2)

    df1 = apply_column_mapping(df1)

    # normalize numeric values
    df1 = normalize_numeric(df1)
    df2 = normalize_numeric(df2)

    # sort columns
    df1 = df1.sort_index(axis=1)
    df2 = df2.sort_index(axis=1)

    # sort rows
    df1 = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
    df2 = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)

    if df1.equals(df2):
        return True, None

    diff = pd.concat([df1, df2]).drop_duplicates(keep=False)

    return False, diff.to_string()