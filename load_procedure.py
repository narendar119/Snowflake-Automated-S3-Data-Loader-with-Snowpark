import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def load_data_to_snowflake(session: snowpark.Session, stage_name: str, file_pattern: str, target_table: str):
    """
    The handler function for the Stored Procedure.
    Reads files from the specified stage/pattern, infers schema, and loads data.
    """
    
    try:
        # 1. Create a Snowpark DataFrame from the Stage
        # INFER_SCHEMA=True is crucial for dynamic table creation
        sf_df = session.read.options({
            'HEADER': True, 
            'INFER_SCHEMA': True
        }).csv(f'@{stage_name}/{file_pattern}')
        
        # 2. Dynamic Table Creation and Data Load
        # 'overwrite' mode drops and recreates the table, ensuring the schema matches the current file structure.
        sf_df.write.mode("overwrite").save_as_table(target_table)
        
        return f"Successfully loaded data from stage {stage_name} (matching {file_pattern}) into table {target_table} with {sf_df.count()} records."
        
    except Exception as e:
        return f"Error during data loading: {e}"
