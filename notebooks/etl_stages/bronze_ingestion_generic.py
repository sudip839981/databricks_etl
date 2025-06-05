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
            final_options = {"header": "true", "inferSchema": "true"}
            final_options.update(format_options) # format_options from config can override defaults
            source_df = spark.read.format("csv").options(**final_options).load(full_adls_path)
        elif dataset_format.upper() == "JSON":
            # JSON specific options (e.g., multiline)
            source_df = spark.read.format("json").options(**format_options).load(full_adls_path)
        elif dataset_format.upper() == "PARQUET":
            source_df = spark.read.format("parquet").options(**format_options).load(full_adls_path)
        elif dataset_format.upper() == "DELTA":
            source_df = spark.read.format("delta").options(**format_options).load(full_adls_path)
        # EXCEL will be handled in a separate block or requires pandas_on_spark / custom solution
        elif dataset_format.upper() == "EXCEL":
            print(f"WARNING: EXCEL format from ADLS_GEN2 is specified. This basic template does not fully implement Excel reading. Requires additional libraries or logic (e.g. pandas integration or koalas). Placeholder for now.")
            # Example using pandas if allowed and file is small enough for driver:
            # if file_path: # pandas typically reads a single file
            #    excel_bytes = dbutils.fs.head(full_adls_path, 1024*1024*10) # Limit size for head, or use direct pandas read if cluster has access.
            #    pandas_df = pd.read_excel(io.BytesIO(excel_bytes), **format_options) # format_options for pandas: sheet_name, header etc.
            #    source_df = spark.createDataFrame(pandas_df)
            # else:
            #    dbutils.notebook.exit("ERROR: EXCEL reading from a folder path is not supported in this basic template.")
            dbutils.notebook.exit(f"ERROR: EXCEL format for ADLS_GEN2 requires specific implementation (e.g., using pandas or similar). DatasetID: {dataset_id}")

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
