# Databricks notebook source

# MAGIC %md
# MAGIC # Populate `DataSources` Configuration Table
# MAGIC
# MAGIC This notebook provides a way to add or update entries in the `config.DataSources` table.
# MAGIC It uses a merge operation to either insert new records or update existing ones based on `DataSourceID`.
# MAGIC
# MAGIC **Important:**
# MAGIC * Ensure the `config.DataSources` table has been created by running the `01_create_config_tables.py` notebook first.
# MAGIC * The user/principal running this notebook must have `MODIFY` permissions on the `config.DataSources` table and `USAGE` on its parent catalog and schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Parameters

# COMMAND ----------

dbutils.widgets.text("target_catalog", "main", "Target Unity Catalog Name")
dbutils.widgets.text("target_schema", "config", "Target Schema Name for 'DataSources' table")

# Widgets for a single DataSource entry
dbutils.widgets.text("data_source_id", "", "DataSourceID (UUID string, e.g., '123e4567-e89b-12d3-a456-426614174000')")
dbutils.widgets.text("source_name", "", "SourceName (e.g., 'ADLS Raw Zone')")
dbutils.widgets.text("source_type", "ADLS_GEN2", "SourceType (e.g., ADLS_GEN2, SQL_SERVER)")
dbutils.widgets.text("connection_details_json", "{}", "ConnectionDetails (JSON string)")
dbutils.widgets.text("description", "", "Optional description")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
full_table_name = f"{target_catalog}.{target_schema}.DataSources"

print(f"Target Table: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for Upsert
# MAGIC
# MAGIC You can modify the `data_to_upsert` list to include multiple entries.
# MAGIC For bulk loading, consider reading from a CSV or JSON file and transforming it into this structure.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, current_timestamp
import uuid

# Get widget values for a single entry
ds_id = dbutils.widgets.get("data_source_id")
if not ds_id: # Generate a UUID if not provided
    ds_id = str(uuid.uuid4())

source_name = dbutils.widgets.get("source_name")
source_type = dbutils.widgets.get("source_type")
connection_details = dbutils.widgets.get("connection_details_json")
description = dbutils.widgets.get("description")

# Basic validation
if not source_name or not source_type:
    dbutils.notebook.exit("SourceName and SourceType are required fields.")

# Schema for the DataSources table (must match the DDL)
schema = StructType([
    StructField("DataSourceID", StringType(), False),
    StructField("SourceName", StringType(), False),
    StructField("SourceType", StringType(), False),
    StructField("ConnectionDetails", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("CreatedAt", TimestampType(), False),
    StructField("UpdatedAt", TimestampType(), False)
])

# Create a list of dictionaries or Row objects for the new data
# This example uses the widget inputs for a single record.
# For multiple records, you would expand this list.
new_data = [
    {
        "DataSourceID": ds_id,
        "SourceName": source_name,
        "SourceType": source_type,
        "ConnectionDetails": connection_details,
        "Description": description,
        # CreatedAt will be set by the merge logic for new records
        # UpdatedAt will be set by the merge logic always
    }
]

if not new_data[0]["SourceName"]: # Check if any actual data was provided via widgets
    print("No data provided in widgets. Skipping upsert. Fill in widget values to add a new entry.")
    dbutils.notebook.exit("No data provided in widgets.")

try:
    source_df = spark.createDataFrame(new_data)
    # Select in the correct order and add missing audit columns for the merge
    source_df = source_df.select(
        lit(new_data[0]["DataSourceID"]).alias("DataSourceID"), # Ensure DataSourceID is correctly passed
        lit(new_data[0]["SourceName"]).alias("SourceName"),
        lit(new_data[0]["SourceType"]).alias("SourceType"),
        lit(new_data[0]["ConnectionDetails"]).alias("ConnectionDetails"),
        lit(new_data[0]["Description"]).alias("Description")
    )
    source_df.show(truncate=False)
except Exception as e:
    print(f"Error creating DataFrame from input: {e}")
    dbutils.notebook.exit(f"Error creating DataFrame: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Merge Operation
# MAGIC
# MAGIC This will insert new records or update existing ones if a matching `DataSourceID` is found.

# COMMAND ----------

# Check if the target table exists
try:
    spark.read.table(full_table_name).limit(1).collect()
    print(f"Target table {full_table_name} exists.")
except Exception as e:
    print(f"ERROR: Target table {full_table_name} does not exist or is not accessible. Please run the `01_create_config_tables.py` notebook first. Details: {e}")
    dbutils.notebook.exit(f"Target table not found: {full_table_name}")

# Perform the merge operation
print(f"Attempting to merge data into {full_table_name}...")

try:
    # For Databricks Runtime 12.2 LTS and above, spark.read.table returns a DataFrame.
    # To use MERGE, we need to ensure we are working with a DeltaTable object if direct merge on DataFrame is not supported or preferred.
    # However, MERGE INTO can be directly executed using spark.sql if the target is a Delta table.
    # The approach below using DataFrame.merge() is idiomatic PySpark for Delta tables.

    # Dynamically construct the merge statement for clarity and to handle SQL injection-like issues if values were directly interpolated (though here we use DataFrame merge which is safe)

    # Need to access the DeltaTable object for merge API
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, full_table_name):
        print(f"ERROR: {full_table_name} is not a Delta table. Merge operations are only supported for Delta tables.")
        dbutils.notebook.exit(f"{full_table_name} is not a Delta table.")

    delta_target_table = DeltaTable.forName(spark, full_table_name)

    (delta_target_table.alias("target")
     .merge(source_df.alias("source"), "target.DataSourceID = source.DataSourceID")
     .whenMatchedUpdate(set={
         "SourceName": "source.SourceName",
         "SourceType": "source.SourceType",
         "ConnectionDetails": "source.ConnectionDetails",
         "Description": "source.Description",
         "UpdatedAt": current_timestamp()
     })
     .whenNotMatchedInsert(values={
         "DataSourceID": "source.DataSourceID",
         "SourceName": "source.SourceName",
         "SourceType": "source.SourceType",
         "ConnectionDetails": "source.ConnectionDetails",
         "Description": "source.Description",
         "CreatedAt": current_timestamp(),
         "UpdatedAt": current_timestamp()
     })
     .execute())

    print(f"Merge operation completed successfully for {full_table_name}.")

except Exception as e:
    print(f"ERROR during merge operation: {e}")
    # Attempt to provide more specific advice for common DeltaTable issues
    if "DeltaTable.forName" in str(e) or "isDeltaTable" in str(e):
        print("Hint: Ensure the delta-spark library is compatible with your Databricks Runtime or that you're using a DBR that natively supports DeltaTable operations.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC Display the contents of the `DataSources` table after the merge.

# COMMAND ----------

print(f"Contents of {full_table_name} after merge:")
spark.read.table(full_table_name).orderBy("UpdatedAt", ascending=False).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to use for bulk updates/inserts:
# MAGIC 1. **Prepare a CSV or JSON file:** Create a file with your DataSource entries.
# MAGIC 2. **Upload to DBFS or cloud storage:** Make the file accessible to your Databricks workspace.
# MAGIC 3. **Read the file into a DataFrame:** Use `spark.read.csv(...)` or `spark.read.json(...)`.
# MAGIC 4. **Transform the DataFrame:** Ensure it matches the `DataSources` schema and column names (`DataSourceID`, `SourceName`, `SourceType`, `ConnectionDetails`, `Description`).
# MAGIC 5. **Use the transformed DataFrame as `source_df`:** Replace the manual `source_df` creation with your DataFrame.
# MAGIC
# MAGIC Example for reading from CSV:
# MAGIC ```python
# MAGIC # csv_path = "/path/to/your/datasources.csv"
# MAGIC # source_df_from_file = spark.read.option("header", "true").csv(csv_path)
# MAGIC #
# MAGIC # # Assuming CSV columns are named correctly, otherwise add .select() or .withColumnRenamed()
# MAGIC # source_df = source_df_from_file.select(
# MAGIC #     col("DataSourceID_from_csv").alias("DataSourceID"),
# MAGIC #     col("SourceName_from_csv").alias("SourceName"),
# MAGIC #     # ... and so on for other columns
# MAGIC # )
# MAGIC ```
