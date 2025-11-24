import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_timestamp
from snowflake.snowpark.types import StringType

def process_stage_files(session: snowpark.Session, stage_name: str):
    """
    Core handler for the Stored Procedure. Automates table creation and data load 
    from S3 stage to Snowflake, skipping already processed files using a log table.
    """
    
    stage_path = f"@{stage_name}"
    
    # 1. Get list of already processed files from the log table
    # This prevents reloading the same file on subsequent runs.
    log_df = session.table("LOG_TABLE").select(col("FILE_NAME")).collect()
    already_loaded_files = {row[0] for row in log_df}
    
    # 2. List all files currently on the stage
    # The LIST command is executed as a SQL query
    list_result_df = session.sql(f"LIST {stage_path}").collect()
    
    files_to_process = []
    
    # 3. Filter and prepare file information
    for row in list_result_df:
        file_path = row['name'] # Example: /customer/customer_1.csv
        
        if file_path in already_loaded_files:
            continue 

        # Extract folder name (used for table name) and file name
        path_parts = [p for p in file_path.split('/') if p] # Splits by /, removing empty strings
        
        if len(path_parts) >= 2:
            folder_name = path_parts[-2].upper()
            file_name = path_parts[-1]
            files_to_process.append({
                'folder': folder_name, 
                'file': file_name, 
                'full_path': f"'{stage_path}{file_path}'" # Full path for COPY INTO command
            })

    if not files_to_process:
        return "No new files to process. All files either logged or stage is empty."

    # 4. Process each new file
    for item in files_to_process:
        target_table = item['folder']
        full_file_path = item['full_path']

        try:
            # 4a. Check if the target table exists using INFORMATION_SCHEMA
            table_exists_query = f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = CURRENT_SCHEMA() AND table_name = '{target_table}'
            """
            table_count = session.sql(table_exists_query).collect()[0][0]
            
            if table_count == 0:
                # 4b. Table does not exist: Infer schema and create table using TEMPLATE
                
                # Get schema definitions as a JSON string
                infer_schema_query = f"""
                    SELECT ARRAY_TO_STRING(ARRAY_AGG(OBJECT_CONSTRUCT(*)), ',') FROM 
                    TABLE(INFER_SCHEMA(location => {full_file_path}, file_format => 'FFL_CSV_SKIP_HEADER'))
                """
                schema_json = session.sql(infer_schema_query).collect()[0][0]
                
                # Create the table using the inferred schema template
                create_table_query = f"CREATE OR REPLACE TABLE {target_table} USING TEMPLATE ({schema_json})"
                session.sql(create_table_query).collect()

            # 4c. Perform the COPY INTO command
            copy_query = f"""
                COPY INTO {target_table}
                FROM {full_file_path}
                FILE_FORMAT = (FORMAT_NAME = 'FFL_CSV_SKIP_HEADER')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'ABORT_STATEMENT'
            """
            session.sql(copy_query).collect()
            
            # 4d. Insert record into the log table
            log_insert_query = f"""
                INSERT INTO LOG_TABLE (FOLDER_NAME, FILE_NAME, LOAD_TIMESTAMP)
                VALUES ('{item['folder']}', '{item['file']}', CURRENT_TIMESTAMP())
            """
            session.sql(log_insert_query).collect()
            
        except Exception as e:
            # This is where the error notification logic would be implemented in a production environment
            print(f"Error processing {item['file']} into {target_table}: {e}")
            
    return "Automated Snowpark load procedure completed. Check LOG_TABLE for results."
