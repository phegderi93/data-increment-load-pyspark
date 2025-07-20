# Data Warehouse Learning Project

This project demonstrates the implementation of a data warehouse using PySpark and Delta Lake. It includes processes for loading data into bronze tables, transforming it into silver tables, and enriching the data for further analysis.

## Project Structure

The project is organized into the following components:

- **`main.py`**: The entry point of the application. It orchestrates the creation of tables, views, and data enrichment processes.
- **`lib/`**: Contains the core modules for managing the data pipeline:
  - **`spark_session.py`**: Manages the Spark session.
  - **`database_manager.py`**: Handles database creation and management.
  - **`bronze_upload.py`**: Handles the ingestion of raw data into bronze tables.
  - **`create_tables.py`**: Creates bronze and silver tables and views.
  - **`data_enrichment.py`**: Enriches data from bronze to silver tables.

## Features

1. **Bronze Tables**: Raw data ingestion.
2. **Silver Tables**: Data transformation and enrichment.
3. **Delta Lake**: Used for table storage and versioning.
4. **Control Table**: Tracks the last updated timestamp for each silver table.
5. **Error Handling**: Ensures robust data processing.

## Prerequisites

- Python 3.12 or higher
- Apache Spark
- Delta Lake
- macOS (or compatible environment)


