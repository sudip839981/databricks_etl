# Databricks notebook source

# MAGIC %md
# MAGIC # Generic Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook ingests data from various configured sources into the Bronze layer of the medallion architecture.
# MAGIC It is driven by metadata stored in the `DataSources` and `Datasets` configuration tables.
# MAGIC
# MAGIC **Responsibilities:**
# MAGIC 1. Read connection and dataset metadata from configuration tables.
# MAGIC 2. Connect to the source system (ADLS Gen2, SQL Server, etc.).
# MAGIC 3. Load data from the specified source (file, folder, table, query).
# MAGIC 4. Add basic audit columns (_bronze_load_timestamp, _source_file_name, _dataset_id).
# MAGIC 5. Write the data as a Delta table into the configured Bronze layer schema in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC
# MAGIC Define parameters for the notebook. These will typically be passed by the orchestrating Databricks Workflow.

# COMMAND ----------

dbutils.widgets.text("dataset_id", "", "DatasetID from config.Datasets to process")
dbutils.widgets.text("config_catalog", "main", "Unity Catalog for configuration tables")
dbutils.widgets.text("config_schema", "config", "Schema for configuration tables")
dbutils.widgets.text("bronze_catalog", "bronze_data", "Target Unity Catalog for Bronze layer tables")
dbutils.widgets.text("bronze_schema", "landing", "Target Schema for Bronze layer tables")
dbutils.widgets.text("processing_date", "", "Optional: Processing date (YYYY-MM-DD) for incremental loads or filtering")

# Retrieve widget values
dataset_id = dbutils.widgets.get("dataset_id")
config_catalog = dbutils.widgets.get("config_catalog")
config_schema = dbutils.widgets.get("config_schema")
bronze_catalog = dbutils.widgets.get("bronze_catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
processing_date = dbutils.widgets.get("processing_date") # Can be empty

# Construct full paths for config tables
CONFIG_DATASOURCES_TABLE = f"{config_catalog}.{config_schema}.DataSources"
CONFIG_DATASETS_TABLE = f"{config_catalog}.{config_schema}.Datasets"

print(f"Starting Bronze ingestion for DatasetID: {dataset_id}")
print(f"Config Tables: {CONFIG_DATASOURCES_TABLE}, {CONFIG_DATASETS_TABLE}")
print(f"Target Bronze Location: {bronze_catalog}.{bronze_schema}")
if processing_date:
    print(f"Processing Date: {processing_date}")

# Basic validation
if not dataset_id:
    dbutils.notebook.exit("ERROR: dataset_id widget must be provided.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Configuration
# MAGIC
# MAGIC Fetch metadata for the given `dataset_id` from `Datasets` and its corresponding `DataSources` record.

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType # Added for explicit casting for _source_file_name
import json

try:
    print(f"Reading configuration for DatasetID: {dataset_id} from {CONFIG_DATASETS_TABLE}")
    dataset_config_df = spark.read.table(CONFIG_DATASETS_TABLE).where(col("DatasetID") == dataset_id)
    dataset_config = dataset_config_df.first()

    if not dataset_config:
        dbutils.notebook.exit(f"ERROR: No configuration found for DatasetID '{dataset_id}' in {CONFIG_DATASETS_TABLE}.")

    print(f"Dataset configuration found: {dataset_config.DatasetName}")

    data_source_id = dataset_config.DataSourceID
    print(f"Reading configuration for DataSourceID: {data_source_id} from {CONFIG_DATASOURCES_TABLE}")
    datasource_config_df = spark.read.table(CONFIG_DATASOURCES_TABLE).where(col("DataSourceID") == data_source_id)
    datasource_config = datasource_config_df.first()

    if not datasource_config:
        dbutils.notebook.exit(f"ERROR: No configuration found for DataSourceID '{data_source_id}' (linked to DatasetID '{dataset_id}') in {CONFIG_DATASOURCES_TABLE}.")

    print(f"DataSource configuration found: {datasource_config.SourceName} (Type: {datasource_config.SourceType})")

except Exception as e:
    print(f"ERROR: Failed to read configuration: {e}")
    raise

# Extract key configuration details
source_type = datasource_config.SourceType
connection_details_json = datasource_config.ConnectionDetails
source_identifier_json = dataset_config.SourceIdentifier # This is a JSON string
dataset_format = dataset_config.DatasetFormat
format_options_json = dataset_config.FormatOptions # This is a JSON string

# Parse JSON strings safely
try:
    connection_details = json.loads(connection_details_json) if connection_details_json else {}
    source_identifier = json.loads(source_identifier_json) if source_identifier_json else {}
    format_options = json.loads(format_options_json) if format_options_json else {}
except json.JSONDecodeError as e:
    dbutils.notebook.exit(f"ERROR: Invalid JSON in configuration tables: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Data from Source
# MAGIC
# MAGIC Based on `SourceType` and `DatasetFormat`, connect to the source and read data.

# COMMAND ----------

import os # For os.path.exists
source_df = None
active_source_identifier_path = None # To store the path actually used, for audit logging source_file_name

print(f"Attempting to read data for SourceType: '{source_type}', DatasetFormat: '{dataset_format}'")

if source_type == "ADLS_GEN2":
    storage_account = connection_details.get("storage_account_name")
    container = connection_details.get("container_name") # This was 'container_name' in DDL notes

    if not storage_account or not container:
        dbutils.notebook.exit(f"ERROR: For ADLS_GEN2, 'storage_account_name' and 'container_name' must be specified in ConnectionDetails for DataSourceID '{data_source_id}'.")

    # Determine if it's a file_path or folder_path from SourceIdentifier
    file_path = source_identifier.get("file_path")
    folder_path = source_identifier.get("folder_path")

    if file_path:
        active_source_identifier_path = file_path
        full_adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{file_path.lstrip('/')}"
        print(f"Reading single file from ADLS Gen2 path: {full_adls_path}")
    elif folder_path:
        active_source_identifier_path = folder_path
        full_adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{folder_path.lstrip('/')}"
        print(f"Reading from ADLS Gen2 folder path: {full_adls_path}")
    else:
        dbutils.notebook.exit(f"ERROR: For ADLS_GEN2, 'file_path' or 'folder_path' must be specified in SourceIdentifier for DatasetID '{dataset_id}'.")

    try:
        reader = spark.read

        # Apply common format options first
        if format_options:
            reader = reader.options(**format_options)

        if dataset_format.upper() == "CSV":
            # CSV specific options can also be in format_options (e.g., header, inferSchema, delimiter)
            # Default to inferSchema=True and header=True if not specified, common for bronze.
            final_options = {"header": "true", "inferSchema": "true", "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record_data"}
            final_options.update(format_options) # format_options from config can override defaults
            source_df = spark.read.format("csv").options(**final_options).load(full_adls_path)
        elif dataset_format.upper() == "JSON":
            # JSON specific options (e.g., multiline)
            json_options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record_data"}
            json_options.update(format_options)
            source_df = spark.read.format("json").options(**json_options).load(full_adls_path)
        elif dataset_format.upper() == "PARQUET":
            source_df = spark.read.format("parquet").options(**format_options).load(full_adls_path)
        elif dataset_format.upper() == "DELTA":
            source_df = spark.read.format("delta").options(**format_options).load(full_adls_path)
        # EXCEL will be handled in a separate block or requires pandas_on_spark / custom solution
        elif dataset_format.upper() == "EXCEL":
            print(f"Attempting to read EXCEL format from ADLS Gen2 path: {full_adls_path}")
            # Excel reading often requires pandas for flexibility (sheet_name, header, etc.)
            # Ensure pandas is available. It's standard in Databricks runtimes.
            import pandas as pd
            import io

            # Default pandas options for read_excel (can be overridden by format_options)
            # None for sheet_name means read the first sheet. header=0 means first row is header.
            pandas_read_options = {
                "sheet_name": 0, # Default to first sheet if not specified
                "header": 0      # Default to first row as header if not specified
            }
            if format_options:
                # User-provided format_options can override defaults or add others like 'skiprows', 'usecols'
                # Example: {"sheet_name": "Sheet2", "header": 0}
                # Example: {"sheet_name": 0, "skiprows": 3, "usecols": "A:D"}
                pandas_read_options.update(format_options)

            try:
                # For direct reading from ADLS with abfss paths, Spark's file system utilities
                # can be used to get the file content if the cluster has appropriate permissions.
                # This approach reads the file into memory on the driver, so suitable for moderately sized files.
                # For very large Excel files, alternative strategies might be needed (e.g., pre-conversion).

                if not file_path: # pandas read_excel typically works best with a single file path
                    dbutils.notebook.exit(f"ERROR: For EXCEL format from ADLS_GEN2, 'file_path' must be specified in SourceIdentifier, not 'folder_path'. DatasetID: {dataset_id}")

                # Read the file content using dbutils.fs.head() or by constructing a pandas-readable path
                # Using direct path with fsspec-compatible libraries if available is often cleaner,
                # but dbutils.fs.cp to local FS and then read is a common robust pattern if direct read is tricky.

                # Let's try a common pattern: copy to a temporary local path.
                # This requires the source path to be a single file.
                temp_local_excel_path = f"/tmp/{dataset_id}_{active_source_identifier_path.split('/')[-1]}"
                dbutils.fs.cp(full_adls_path, f"file:{temp_local_excel_path}") # Copy from abfss to local file API

                print(f"Copied Excel file to temporary local path: {temp_local_excel_path} for pandas processing.")
                print(f"Pandas read_excel options: {pandas_read_options}")

                pandas_df = pd.read_excel(temp_local_excel_path, **pandas_read_options)

                # Clean up the temporary local file
                dbutils.fs.rm(f"file:{temp_local_excel_path}")

                # Convert pandas DataFrame to Spark DataFrame
                # Handle potential issues with schema inference if necessary (e.g. all-string types)
                if pandas_df.empty:
                    print(f"WARNING: Pandas DataFrame read from Excel file {full_adls_path} (sheet: {pandas_read_options.get('sheet_name')}) is empty.")
                    # Create an empty Spark DataFrame with schema if possible, or exit/warn based on policy
                    # For now, let Spark infer schema from empty df, which might lead to no columns.
                    # Consider defining schema from BronzeLayerSchema config if available and df is empty.
                    source_df = spark.createDataFrame(pandas_df)
                else:
                    source_df = spark.createDataFrame(pandas_df)

                print(f"Successfully read Excel file {full_adls_path} using pandas and converted to Spark DataFrame.")

            except Exception as e_excel:
                # Ensure temporary file is cleaned up even if pandas read fails
                if 'temp_local_excel_path' in locals() and os.path.exists(temp_local_excel_path.replace("file:","")): # check local path for os.exists
                    try:
                        dbutils.fs.rm(f"file:{temp_local_excel_path}")
                    except Exception as e_cleanup:
                        print(f"WARNING: Failed to clean up temporary local Excel file {temp_local_excel_path}: {e_cleanup}")

                print(f"ERROR: Failed to read or process EXCEL file {full_adls_path}. DatasetID: {dataset_id}. Error: {e_excel}")
                raise e_excel # Re-raise the exception to fail the notebook.
        else:
            dbutils.notebook.exit(f"ERROR: Unsupported DatasetFormat '{dataset_format}' for SourceType 'ADLS_GEN2'. DatasetID: {dataset_id}")

        print(f"Successfully initiated read for {dataset_format} from {full_adls_path}")

    except Exception as e:
        print(f"ERROR: Failed to read data from ADLS Gen2 path {full_adls_path} for DatasetID '{dataset_id}'. Error: {e}")
        raise

elif source_type == "SQL_SERVER":
    db_server = connection_details.get("server_name")
    db_name = connection_details.get("database_name")
    db_user = connection_details.get("username")
    # Password should be stored as a secret in Azure Key Vault, and config should store the secret name and scope
    akv_scope_name = connection_details.get("akv_secret_scope") # e.g., "kv-scope"
    akv_secret_key_for_password = connection_details.get("akv_secret_key_for_password") # e.g., "sql-server-password"
    db_port = connection_details.get("port", 1433) # Default SQL Server port

    if not all([db_server, db_name, db_user, akv_scope_name, akv_secret_key_for_password]):
        dbutils.notebook.exit(f"ERROR: For SQL_SERVER, 'server_name', 'database_name', 'username', 'akv_secret_scope', and 'akv_secret_key_for_password' must be specified in ConnectionDetails for DataSourceID '{data_source_id}'.")

    try:
        db_password = dbutils.secrets.get(scope=akv_scope_name, key=akv_secret_key_for_password)
    except Exception as e:
        dbutils.notebook.exit(f"ERROR: Failed to retrieve SQL Server password from Azure Key Vault. Scope: '{akv_scope_name}', Key: '{akv_secret_key_for_password}'. Error: {e}")

    jdbc_url = f"jdbc:sqlserver://{db_server}:{db_port};databaseName={db_name}"

    # Determine if reading from a table/view or a custom query
    db_schema_name = source_identifier.get("schema_name", "dbo") # Default to 'dbo' if not specified
    table_or_view_name = source_identifier.get("table_or_view_name")
    custom_query = source_identifier.get("query")

    if custom_query:
        # Use the custom query directly. It needs to be wrapped in parentheses and aliased for Spark JDBC.
        # Example: (SELECT col1, col2 FROM mytable WHERE condition) AS subquery
        if not (custom_query.strip().startswith("(") and custom_query.strip().endswith(")")):
             # Basic check, might need more sophisticated validation depending on query complexity
            query_to_run = f"({custom_query}) AS custom_query_alias"
        else:
            query_to_run = custom_query # Assume user provided it correctly aliased if parentheses are present
        print(f"Reading from SQL Server using custom query: {query_to_run}")
    elif table_or_view_name:
        query_to_run = f"{db_schema_name}.{table_or_view_name}"
        print(f"Reading from SQL Server table/view: {query_to_run}")
    else:
        dbutils.notebook.exit(f"ERROR: For SQL_SERVER, 'table_or_view_name' or 'query' must be specified in SourceIdentifier for DatasetID '{dataset_id}'.")

    try:
        reader_options = {
            "url": jdbc_url,
            "user": db_user,
            "password": db_password,
            # "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver" # Optional: Spark usually infers this
        }

        # Add any format_options from config (e.g., fetchsize, queryTimeout)
        # These would be specific to JDBC driver options.
        if format_options:
            reader_options.update(format_options)

        # For table/view, use "dbtable". For a query, also use "dbtable".
        reader_options["dbtable"] = query_to_run

        source_df = spark.read.format("jdbc").options(**reader_options).load()
        print(f"Successfully initiated read from SQL Server: {query_to_run}")

    except Exception as e:
        print(f"ERROR: Failed to read data from SQL Server for DatasetID '{dataset_id}'. Query/Table: {query_to_run}. Error: {e}")
        raise

# Add more source types as needed (e.g., API, FTP)

else:
    dbutils.notebook.exit(f"ERROR: Unsupported SourceType '{source_type}' encountered.")

if source_df is None:
    dbutils.notebook.exit(f"ERROR: source_df was not populated. Check data reading logic for SourceType '{source_type}' and DatasetFormat '{dataset_format}'.")

print("Data read successfully from source.")
source_df.printSchema()
# source_df.show(5, truncate=False) # Uncomment for debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Handle Corrupt Records (if applicable)
# MAGIC
# MAGIC For sources like CSV/JSON read in PERMISSIVE mode, check for and quarantine corrupt records.

# COMMAND ----------

corrupt_record_column_name = "_corrupt_record_data" # Must match what was used in reader options

if corrupt_record_column_name in source_df.columns:
    print(f"Checking for corrupt records in column: {corrupt_record_column_name}")

    corrupt_records_df = source_df.where(col(corrupt_record_column_name).isNotNull())
    good_records_df = source_df.where(col(corrupt_record_column_name).isNull())

    if not corrupt_records_df.rdd.isEmpty(): # isEmpty() is more robust for checking if a DataFrame has data
        print(f"Found corrupt records for DatasetID: {dataset_id}")

        # Define quarantine table name (can be made more configurable later)
        quarantine_table_name_suffix = dataset_config.DatasetName.lower().replace(' ', '_').replace('-', '_')
        quarantine_table_full_name = f"{bronze_catalog}.{bronze_schema}.brz_quarantined_{quarantine_table_name_suffix}"

        print(f"Writing corrupt records to: {quarantine_table_full_name}")

        # Select necessary fields for quarantine. Add source reference for traceability.
        source_ref_for_quarantine = "unknown_source" # Default value
        if source_type == "ADLS_GEN2" and 'active_source_identifier_path' in locals() and active_source_identifier_path:
            source_ref_for_quarantine = active_source_identifier_path
        elif source_type == "SQL_SERVER":
            # Check if custom_query or table_or_view_name are in scope from the SQL_SERVER block
            # These might not be if this cell is run independently or if SQL_SERVER block wasn't executed
            current_custom_query = source_identifier.get("query") # Re-fetch from source_identifier for safety
            current_table_or_view_name = source_identifier.get("table_or_view_name")
            current_db_schema_name = source_identifier.get("schema_name", "dbo")

            if current_custom_query:
                source_ref_for_quarantine = f"sql_query(datasource:{datasource_config.SourceName})"
            elif current_table_or_view_name:
                source_ref_for_quarantine = f"sql_table:{current_db_schema_name}.{current_table_or_view_name}"


        quarantined_data_to_write = corrupt_records_df.select(
            lit(dataset_id).alias("DatasetID"),
            lit(source_ref_for_quarantine).alias("SourceReference"),
            col(corrupt_record_column_name).alias("CorruptRecordData"),
            current_timestamp().alias("LoadTimestamp")
        )

        try:
            # Ensure target quarantine schema (same as bronze schema for now) exists
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{bronze_schema}")

            quarantined_data_to_write.write.format("delta").mode("append").saveAsTable(quarantine_table_full_name)

            # Re-count after write attempt for accurate logging if needed, or use value from a more direct count action
            # For now, this count is before write, if write fails, count is still of attempt.
            # corrupt_count = quarantined_data_to_write.count() # This would be another job
            # print(f"Successfully wrote {corrupt_count} corrupt records to {quarantine_table_full_name}")
            # To avoid extra job for count, we can assume if no exception, all rows in quarantined_data_to_write were written.
            # A more robust way if count is critical, is to count before write.

            # The corrupt_records_df.rdd.isEmpty() check implies there are rows if false.
            # A direct count before write is safer if exact number is needed for logging here.
            num_corrupt_records = corrupt_records_df.count() # Get count for logging
            print(f"Successfully wrote {num_corrupt_records} corrupt records to {quarantine_table_full_name}")

        except Exception as e_quarantine:
            print(f"ERROR: Failed to write corrupt records to {quarantine_table_full_name}. Error: {e_quarantine}")
            # Decide if this should be a fatal error for the whole job. For now, log and continue with good records.
            # Consider adding a notification here.

        # Update source_df to only contain good records
        source_df = good_records_df.drop(corrupt_record_column_name) # Drop the now-empty corrupt record column
        print("Proceeding with good records.")
    else:
        print("No corrupt records found.")
        if corrupt_record_column_name in source_df.columns: # Ensure column is dropped if it exists but all were null
            source_df = source_df.drop(corrupt_record_column_name)

else:
    print(f"Corrupt record column '{corrupt_record_column_name}' not found in DataFrame. Skipping corrupt record check (may not be applicable for this source type/format or options).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Check for Empty Source Data
# MAGIC
# MAGIC After attempting to read and handle corrupt records, check if the resulting DataFrame is empty.

# COMMAND ----------

if source_df.isEmpty():
    print(f"WARNING: Source data for DatasetID '{dataset_id}' (Source Type: '{source_type}', Format: '{dataset_format}') is empty after all read attempts and corrupt record handling.")
    # Depending on desired policy, you might:
    # 1. Continue processing (writes an empty table to Bronze) - Current implicit behavior.
    # 2. Exit gracefully: dbutils.notebook.exit(f"INFO: Source for DatasetID '{dataset_id}' is empty. No data to process.")
    # 3. Make this configurable via a setting in the Datasets table.
    # For now, we will just log the warning and allow the process to continue, which will result in an empty table if it's an initial load,
    # or no change if appending (though Bronze is usually overwrite).
    # This ensures the Bronze table is created even if the first load is empty, which can be useful for downstream dependencies.

    # No specific action to stop processing is taken here, allowing an empty DataFrame to proceed.
    # Audit columns will still be added, and an empty table will be written.
    pass # Explicitly noting that we are allowing empty DF to proceed.
else:
    print("Source DataFrame is not empty. Proceeding with audit column addition.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Audit Columns

# COMMAND ----------

print("Adding audit columns...")
# Add fixed audit columns first
audited_df = source_df.withColumn("_bronze_load_timestamp", current_timestamp())                       .withColumn("_dataset_id", lit(dataset_id))

# Determine the value for a source reference audit column
source_reference_value = None
if source_type == "ADLS_GEN2" and active_source_identifier_path:
    if file_path:
        source_reference_value = active_source_identifier_path.split('/')[-1] # Log file name
    else:
        source_reference_value = active_source_identifier_path # Log folder path
elif source_type == "SQL_SERVER":
    if custom_query:
        # For custom queries, log a generic reference or a hash if it's too long
        source_reference_value = f"sql_query(datasource:{datasource_config.SourceName})"
    elif table_or_view_name:
        source_reference_value = f"sql_table:{db_schema_name}.{table_or_view_name}"

if source_reference_value:
    audited_df = audited_df.withColumn("_source_reference", lit(source_reference_value))
else:
    audited_df = audited_df.withColumn("_source_reference", lit(None).cast(StringType())) # Ensure column exists

print("Audit columns added.")
audited_df.printSchema()
# audited_df.show(5, truncate=False) # Uncomment for debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Bronze Layer
# MAGIC
# MAGIC Write the DataFrame as a Delta table into the configured Bronze Unity Catalog schema.

# COMMAND ----------

# Use BronzeLayerTargetTableName from Datasets config as the target table name
bronze_table_name = dataset_config.BronzeLayerTargetTableName
if not bronze_table_name:
    # Fallback to a derived name if not specified, though it should ideally be configured
    # Sanitize DatasetName for use in a table name (e.g., replace spaces, hyphens with underscores)
    sanitized_dataset_name = dataset_config.DatasetName.lower().replace(' ', '_').replace('-', '_')
    derived_name = f"brz_{sanitized_dataset_name}"
    print(f"WARNING: BronzeLayerTargetTableName not specified in config for DatasetID '{dataset_id}'. Falling back to derived name: {derived_name}")
    bronze_table_name = derived_name

if not bronze_table_name: # This case should ideally not be reached if DatasetName is always valid and present
     dbutils.notebook.exit(f"ERROR: Cannot determine Bronze table name for DatasetID '{dataset_id}'. Configure BronzeLayerTargetTableName or ensure DatasetName is valid and populated in the Datasets configuration.")

full_bronze_table_path = f"{bronze_catalog}.{bronze_schema}.{bronze_table_name}"

print(f"Writing data to Bronze table: {full_bronze_table_path}")

try:
    # Ensure catalog and schema exist (though ideally pre-created)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {bronze_catalog}")
    spark.sql(f"USE CATALOG {bronze_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
    # spark.sql(f"USE SCHEMA {bronze_schema}") # USE already done by full_bronze_table_path resolution

    audited_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_bronze_table_path)

    print(f"Successfully wrote data to {full_bronze_table_path}")

except Exception as e:
    print(f"ERROR: Failed to write data to Bronze table {full_bronze_table_path}: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging and Completion

# COMMAND ----------

# Example: Log metrics or summary
# row_count = audited_df.count() # This would trigger an extra Spark job, use with caution on very large DFs.
# print(f"Ingestion complete for DatasetID '{dataset_id}'. Approximated rows processed (if source_df was cached and counted).")

dbutils.notebook.exit(f"Successfully completed Bronze ingestion for DatasetID: {dataset_id}")
