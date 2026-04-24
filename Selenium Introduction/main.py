import time
import os
import csv
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class SeleniumWebDriverContextManager:
    def __init__(self):
        self.driver: WebDriver = None

    def __enter__(self):
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")

        self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            self.driver.quit()


# --------------------------
# TABLE EXTRACTION
# --------------------------
def extract_table(driver: WebDriver):
    try:
        # 1. Wait using CLASS_NAME
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "table"))
        )

        # 2. Get table root using CLASS_NAME
        table = driver.find_element(By.CLASS_NAME, "table")

        # 3. Get columns using XPATH (NEW)
        columns = table.find_elements(By.XPATH, ".//*[@class='y-column']")

        headers = []
        column_data = []

        for col in columns:
            try:
                # 4. Header using ID
                header = col.find_element(By.ID, "header").text.strip()
                headers.append(header)

                # 5. Cells using CSS SELECTOR (NEW)
                cells = col.find_elements(By.CSS_SELECTOR, ".cell-text")

                values = []
                for cell in cells:
                    text = cell.text.strip()
                    if text and text != header:
                        values.append(text)

                column_data.append(values)

            except NoSuchElementException:
                continue

        # transpose columns → rows
        rows = list(zip(*column_data))

        df = pd.DataFrame(rows, columns=headers)
        df.to_csv("table.csv", index=False)

        print("Table saved to table.csv")

    except TimeoutException:
        print("Table not found or not loaded.")


# --------------------------
# DOUGHNUT CHART EXTRACTION
# --------------------------
def extract_doughnut_data(driver: WebDriver, index: int):
    try:
        doughnut = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "pielayer"))
        )

        labels = doughnut.find_elements(
            By.CSS_SELECTOR, "text.slicetext[data-notex='1']"
        )

        data = []

        for label in labels:
            tspans = label.find_elements(By.TAG_NAME, "tspan")
            if len(tspans) >= 2:
                category = tspans[0].text.strip()
                value = tspans[1].text.strip()
                data.append([category, value])

        file_name = f"doughnut{index}.csv"
        with open(file_name, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Category", "Value"])
            writer.writerows(data)

        print(f"Doughnut data saved to {file_name}")

    except TimeoutException:
        print("Doughnut chart not found.")


def process_doughnut(driver: WebDriver):
    try:
        # wait for legend
        legend = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "scrollbox"))
        )

        items = legend.find_elements(By.CLASS_NAME, "traces")

        # initial screenshot + data
        driver.save_screenshot("screenshot0.png")
        extract_doughnut_data(driver, 0)

        index = 1

        for item in items:
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", item)
                time.sleep(0.5)

                item.click()
                time.sleep(1)  # allow chart to update

                driver.save_screenshot(f"screenshot{index}.png")
                extract_doughnut_data(driver, index)

                index += 1

            except Exception as e:
                print(f"Error clicking filter: {e}")

    except TimeoutException:
        print("Legend (filters) not found.")


# --------------------------
# MAIN
# --------------------------
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "report.html")
    file_url = "file:///" + file_path.replace("\\", "/")

    print(f"Loading: {file_url}")
    with SeleniumWebDriverContextManager() as driver:
        driver.get(file_url)
        time.sleep(2)  # initial load

        extract_table(driver)
        process_doughnut(driver)
