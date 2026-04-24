*** Settings ***
Library    SeleniumLibrary
Library    OperatingSystem
Library    Collections
Library    BuiltIn
Library    helper.py

Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}      ${CURDIR}${/}report.html
${PARQUET_FOLDER}   ${CURDIR}${/}parquet_data${/}facility_type_avg_time_spent_per_visit_date
${FILTER_DATE}      2026-04-22

*** Test Cases ***
Compare HTML Table With Parquet Data
    Open Browser    file://${REPORT_FILE}    chrome
    Wait Until Element Is Visible    css:.table
    ${html_df}=    Read Html Table    ${FILTER_DATE}

    ${parquet_df}=    Read Parquet Dataset    ${PARQUET_FOLDER}    ${FILTER_DATE}

    ${match}    ${diff}=    Compare Dataframes    ${html_df}    ${parquet_df}

    Run Keyword If    ${match}    Log    ✅ Data matches perfectly
    Run Keyword If    not ${match}    Fail    ❌ Data mismatch:\n${diff}
