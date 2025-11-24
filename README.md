# Snowflake Automated S3 Data Loader with Snowpark

This project implements an automated, scalable data ingestion pipeline in **Snowflake** using a **Snowpark Python Stored Procedure**. It dynamically connects to an AWS S3 external stage, infers the schema of the files, and loads the data into a target table, leveraging Snowpark's DataFrame API for processing entirely within Snowflake's environment.

## Architecture & Flow

1.  **S3 Bucket:** Hosts raw data files (e.g., CSV).
2.  **Snowflake External Stage:** Configured to point to the S3 location via a **Storage Integration**.
3.  **Snowpark Python Stored Procedure:**
    * Accepts the Stage Name, File Pattern, and Target Table Name as arguments.
    * Creates a Snowpark DataFrame to read data directly from the stage.
    * Infers the schema and creates/overwrites the target table.
    * Loads data from the DataFrame into the final Snowflake table.

## Prerequisites

* An active Snowflake Account.
* An AWS S3 Bucket containing source data.
* A Snowflake **Storage Integration** connecting Snowflake to S3.
* A Snowflake **Warehouse** for compute.

## Setup & Execution

Use the `setup.sql` file to create the necessary Snowflake objects and the Stored Procedure shell. The `load_procedure.py` file contains the core Python logic that should be placed inside the Stored Procedure definition in the SQL file.
