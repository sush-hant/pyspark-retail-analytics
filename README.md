# PySpark Retail Data Processing and Sales Analysis

## Project Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline built using PySpark on a Databricks environment. It processes raw retail transaction data from a CSV file, performs extensive data cleaning and transformation, aggregates sales figures, identifies top-performing products on a monthly basis, and finally stores the processed insights into a partitioned Delta Lake table for efficient querying and analysis.

The goal is to transform, real-world retail data into a clean, structured format suitable for business intelligence and reporting, specifically focusing on identifying monthly top-selling products based on revenue.

## Key Features

*   **Data Ingestion:** Reads raw retail data from a large CSV file (> 1 Million records).
*   **Data Cleaning:**
    *   Handles column renaming for consistency.
    *   Filters out cancelled orders (Invoices starting with 'C').
    *   Removes records with missing essential identifiers (CustomerID).
    *   Filters out transactions with invalid quantities or prices (<= 0).
    *   De-duplicates records to ensure data integrity.
*   **Data Transformation:**
    *   Parses and standardizes `InvoiceDate` into a date format, extracting `Year` and `Month` columns for partitioning and time-based analysis.
    *   Calculates `Revenue` per transaction line (Quantity * Price), rounding to two decimal places.
*   **Data Aggregation:** Groups data by Year, Month, Stockcode, and Description to calculate total monthly revenue (`TotalRevenue`) for each product.
*   **Analysis & Ranking:** Utilizes Spark SQL Window functions (`row_number()`, `partitionBy`, `orderBy`) to rank products within each month based on their `TotalRevenue`.
*   **Insight Generation:** Filters the ranked data to identify the Top 10 revenue-generating products for each month.
*   **Efficient Storage:** Writes the final Top 10 product insights to a Delta Lake table, partitioned by `Year` and `Month` for optimized query performance.

## Technologies Used

*   **Apache Spark:** Core processing engine.
*   **PySpark:** Python API for Spark.
*   **PySpark SQL:** DataFrame API and Spark SQL functions (including Window Functions).
*   **Azure Databricks:**  The execution environment for the notebook.
*   **Azure Data Lake:**  Store the data into Raw and Processed layers
*   **Azure Key Vault:** Safely stores the access keys of the data lake

## Dataset

*   **Source:** retail transaction data(UCI)
*   **Format:** CSV (`/mnt/raw/merge_csv.csv`)
*   **Initial Size:** > 1 Million records.

## ETL Pipeline Steps

1.  **Import Libraries:** Import necessary PySpark SQL functions and Window class.
2.  **Extract:** Load the raw data from `/mnt/raw/merge_csv.csv` into a Spark DataFrame, inferring the schema and using the header.
3.  **Initial Transformation:** Rename `Customer ID` to `CustomerID` for easier referencing.
4.  **Cleanse - Filter Cancellations:** Remove rows where the `Invoice` starts with 'C'.
5.  **Cleanse - Filter Nulls:** Remove rows where `CustomerID` is null.
6.  **Cleanse - Filter Invalid Values:** Remove rows where `Quantity` or `Price` are less than or equal to zero.
7.  **Cleanse - Deduplicate:** Remove exact duplicate rows.
8.  **Transform - Date Processing:**
    *   Convert `InvoiceDate` string (format `MM/d/yyyy H:mm`) to a Date type.
    *   Extract `Year` and `Month` from the standardized `InvoiceDate`.
9.  **Transform - Feature Engineering:** Calculate `Revenue` by multiplying `Quantity` and `Price`, rounding the result.
10. **Aggregate:** Group the cleaned data by `Year`, `Month`, `Stockcode`, and `Description`. Calculate the sum of `Revenue` for each group, aliasing it as `TotalRevenue`.
11. **Analyze - Rank Products:**
    *   Define a Window partitioned by `Year` and `Month`, ordered by `TotalRevenue` descending.
    *   Apply the `row_number()` function over the window to assign a rank to each product within its month.
12. **Load - Filter Top 10:** Filter the ranked DataFrame to keep only rows where `Rank` is less than or equal to 10.
13. **Load - Save to Delta:** Write the resulting `top10_product` DataFrame to a Delta Lake table located at `/mnt/processed/top10_product`, overwriting existing data and partitioning by `Year` and `Month`.



