-- SQL Setup and Stored Procedure Shell

-- Use or create a dedicated environment
USE ROLE SYSADMIN; 
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'MEDIUM';
CREATE DATABASE IF NOT EXISTS DATA_DB;
CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA DATA_DB.RAW_DATA;

-- **Create File Format** (Assuming CSV with header)
CREATE OR REPLACE FILE FORMAT FFL_CSV_SKIP_HEADER
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_DELIMITER = ',';

-- **Create External Stage** -- NOTE: Replace 'MY_STORAGE_INTEGRATION' and 's3://your-bucket-name/data-folder/'
CREATE OR REPLACE STAGE S3_EXTERNAL_STAGE
    URL = 's3://your-bucket-name/data-folder/'
    STORAGE_INTEGRATION = MY_STORAGE_INTEGRATION
    FILE_FORMAT = FFL_CSV_SKIP_HEADER;

-- **Create the Stored Procedure (SP)**
-- The full Python code from the 'load_procedure.py' file must be copied 
-- and pasted between the AS $$ and $$; delimiters below.
CREATE OR REPLACE PROCEDURE AUTOMATE_S3_LOAD_SP(STAGE_NAME VARCHAR, FILE_PATTERN VARCHAR, TARGET_TABLE VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'load_data_to_snowflake'
AS
$$
-- PASTE CONTENTS OF load_procedure.py HERE
$$;

-- Example Execution
-- Replace values as needed to test the procedure once created
-- CALL AUTOMATE_S3_LOAD_SP('S3_EXTERNAL_STAGE', 'data/sales_2025.*csv', 'SALES_DATA_SNOWPARK_TABLE');
