# Databricks notebook source

# MAGIC %md
# MAGIC # Generic Data Output Notebook
# MAGIC
# MAGIC This notebook reads a specified source table (typically from Bronze, Silver, or Gold layers)
# MAGIC and writes it to one or more destinations as configured in the `config.OutputDestinations` table.
# MAGIC
# MAGIC **Supported Destinations:**
# MAGIC - Unity Catalog Tables
# MAGIC - ADLS Gen2 Paths (Delta, Parquet, CSV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog of the source table to be output")
dbutils.widgets.text("source_schema", "", "Schema of the source table to be output")
dbutils.widgets.text("source_table_name", "", "Name of the source table to be output")

# User can specify either a dataset_id (to process all its configured outputs)
# or a specific output_destination_id to process a single output.
dbutils.widgets.text("dataset_id_for_outputs", "", "Optional: DatasetID whose configured outputs should be processed")
dbutils.widgets.text("output_destination_id", "", "Optional: Specific OutputDestinationID to process")

dbutils.widgets.text("config_catalog", "main", "Unity Catalog for configuration tables")
dbutils.widgets.text("config_schema", "config", "Schema for configuration tables")

# Retrieve widget values
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
source_table_name = dbutils.widgets.get("source_table_name")
dataset_id_for_outputs = dbutils.widgets.get("dataset_id_for_outputs")
output_destination_id = dbutils.widgets.get("output_destination_id")
config_catalog = dbutils.widgets.get("config_catalog")
config_schema = dbutils.widgets.get("config_schema")

# Construct full paths for config tables
CONFIG_OUTPUT_DESTINATIONS_TABLE = f"{config_catalog}.{config_schema}.OutputDestinations"
CONFIG_DATASETS_TABLE = f"{config_catalog}.{config_schema}.Datasets" # Needed if using dataset_id_for_outputs

# Basic validation
if not (source_catalog and source_schema and source_table_name):
    dbutils.notebook.exit("ERROR: source_catalog, source_schema, and source_table_name must be provided.")
if not dataset_id_for_outputs and not output_destination_id:
    dbutils.notebook.exit("ERROR: Either dataset_id_for_outputs or output_destination_id must be provided.")
if dataset_id_for_outputs and output_destination_id:
    dbutils.notebook.exit("ERROR: Provide either dataset_id_for_outputs or output_destination_id, not both.")

full_source_table_path = f"{source_catalog}.{source_schema}.{source_table_name}"
print(f"Source table for output: {full_source_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Output Configuration(s)

# COMMAND ----------

from pyspark.sql.functions import col, lit
import json

output_configs_to_process = []

try:
    output_dest_df = spark.read.table(CONFIG_OUTPUT_DESTINATIONS_TABLE)

    if output_destination_id:
        print(f"Fetching configuration for specific OutputDestinationID: {output_destination_id}")
        config_row = output_dest_df.where((col("OutputDestinationID") == output_destination_id) & (col("IsEnabled") == True)).first()
        if config_row:
            output_configs_to_process.append(config_row.asDict())
        else:
            dbutils.notebook.exit(f"ERROR: No enabled configuration found for OutputDestinationID '{output_destination_id}'.")

    elif dataset_id_for_outputs:
        # This assumes OutputDestinations.DatasetID links to the source dataset whose data is being output.
        # Example: If we are outputting a Gold table derived from a specific source Dataset.
        print(f"Fetching enabled output configurations for DatasetID: {dataset_id_for_outputs}")
        # We also need to ensure this output is for the current source_table_name.
        # This link might be implicit (e.g. output for a gold table that IS the DatasetID's gold target)
        # or explicit if OutputDestinations has SourceTableName, SourceLayer fields.
        # For now, assume any output config for the dataset_id is relevant if the source table matches expectations.

        # A better way might be to link OutputDestinations to the Dataset that *produces* the table being output.
        # E.g., if source_table is a gold table, dataset_id_for_outputs should be the ID of that gold dataset.

        # Let's assume DatasetID in OutputDestinations refers to the Dataset definition
        # that logically owns this output specification.
        # The OutputLayer field in OutputDestinations can be used to match if the source_table_name
        # corresponds to what's expected for that DatasetID at that layer.

        # For now, a simpler filter:
        configs = output_dest_df.where((col("DatasetID") == dataset_id_for_outputs) & (col("IsEnabled") == True)).collect()
        if configs:
            for r in configs:
                 output_configs_to_process.append(r.asDict())
        else:
            print(f"WARNING: No enabled output configurations found for DatasetID '{dataset_id_for_outputs}'. No outputs will be processed.")
            # dbutils.notebook.exit() # Or just complete successfully with no action.

    if not output_configs_to_process:
        print("No output configurations to process. Exiting.")
        dbutils.notebook.exit("No output configurations found or enabled for the given parameters.")

    print(f"Found {len(output_configs_to_process)} output destination(s) to process.")

except Exception as e:
    print(f"ERROR: Failed to read OutputDestinations configuration: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Source Table Data

# COMMAND ----------

print(f"Loading source table data from: {full_source_table_path}")
try:
    source_df = spark.read.table(full_source_table_path)
    source_df.printSchema()
    # source_df.show(5, truncate=False) # For debugging
except Exception as e:
    print(f"ERROR: Failed to read source table {full_source_table_path}. Error: {e}")
    raise

if source_df.isEmpty():
    print(f"WARNING: Source table {full_source_table_path} is empty. Outputs will be empty or not created depending on write mode.")
    # Allow processing of empty dataframe.
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Destination(s)

# COMMAND ----------

successful_outputs = 0
failed_outputs = 0

for i, output_conf in enumerate(output_configs_to_process):
    output_id = output_conf.get("OutputDestinationID")
    dest_type = output_conf.get("DestinationType")
    dest_details_json = output_conf.get("DestinationDetails")

    print(f"\nProcessing output {i+1}/{len(output_configs_to_process)}: OutputDestinationID '{output_id}', Type '{dest_type}'")

    if not dest_details_json:
        print(f"ERROR: DestinationDetails are missing for OutputDestinationID '{output_id}'. Skipping.")
        failed_outputs += 1
        continue

    try:
        dest_details = json.loads(dest_details_json)
    except json.JSONDecodeError as e_json:
        print(f"ERROR: Invalid JSON in DestinationDetails for OutputDestinationID '{output_id}'. Error: {e_json}. Skipping.")
        failed_outputs += 1
        continue

    try:
        if dest_type.upper() == "UNITY_CATALOG_TABLE":
            # Details: {"catalog_name": "...", "schema_name": "...", "table_name": "...", "write_mode": "overwrite/append"}
            target_cat = dest_details.get("catalog_name")
            target_sch = dest_details.get("schema_name")
            target_tbl = dest_details.get("table_name")
            write_mode = dest_details.get("write_mode", "overwrite") # Default to overwrite

            if not all([target_cat, target_sch, target_tbl]):
                print(f"ERROR: For UNITY_CATALOG_TABLE, 'catalog_name', 'schema_name', 'table_name' are required in DestinationDetails. OutputDestinationID '{output_id}'. Skipping.")
                failed_outputs +=1
                continue

            full_target_uc_table = f"{target_cat}.{target_sch}.{target_tbl}"
            print(f"Writing to Unity Catalog table: {full_target_uc_table}, Mode: {write_mode}")

            spark.sql(f"CREATE CATALOG IF NOT EXISTS `{target_cat}`")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_cat}`.`{target_sch}`")

            writer = source_df.write.format("delta").mode(write_mode)
            if write_mode.lower() == "overwrite":
                writer = writer.option("overwriteSchema", "true")

            writer.saveAsTable(full_target_uc_table)
            print(f"Successfully wrote to {full_target_uc_table}")
            successful_outputs += 1

        elif dest_type.upper() == "ADLS_GEN2_PATH":
            # Details: {"storage_account_name": "...", "container_name": "...", "path": "/output/fact_sales/",
            #           "format": "DELTA/PARQUET/CSV", "write_mode": "overwrite/append", "options": {"header":"true"}}
            storage_account = dest_details.get("storage_account_name")
            container = dest_details.get("container_name")
            adls_path = dest_details.get("path") # Relative to container root
            output_format = dest_details.get("format", "delta").lower()
            write_mode = dest_details.get("write_mode", "overwrite") # Default to overwrite
            output_options = dest_details.get("options", {}) # e.g. for CSV: {"header": "true", "delimiter": ","}

            if not all([storage_account, container, adls_path]):
                print(f"ERROR: For ADLS_GEN2_PATH, 'storage_account_name', 'container_name', 'path' are required in DestinationDetails. OutputDestinationID '{output_id}'. Skipping.")
                failed_outputs +=1
                continue

            full_adls_target_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{adls_path.lstrip('/')}"
            print(f"Writing to ADLS Gen2 path: {full_adls_target_path}, Format: {output_format}, Mode: {write_mode}")

            writer = source_df.write.format(output_format).mode(write_mode)
            if output_options and isinstance(output_options, dict):
                writer = writer.options(**output_options)

            writer.save(full_adls_target_path)
            print(f"Successfully wrote to {full_adls_target_path}")
            successful_outputs += 1

        else:
            print(f"ERROR: Unsupported DestinationType '{dest_type}' for OutputDestinationID '{output_id}'. Skipping.")
            failed_outputs += 1
            continue

    except Exception as e_write:
        print(f"ERROR writing for OutputDestinationID '{output_id}'. Type: '{dest_type}'. Error: {e_write}")
        failed_outputs += 1
        # Consider adding to a list of failed output objects for summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Completion Summary

# COMMAND ----------

print(f"Data output processing complete.")
print(f"Successfully processed outputs: {successful_outputs}")
print(f"Failed outputs: {failed_outputs}")

if failed_outputs > 0:
    # Potentially exit with error if any output fails, or make this configurable.
    # For now, just log and exit successfully if at least one was successful or no critical error.
    print(f"WARNING: {failed_outputs} output(s) failed. Check logs above.")
    # dbutils.notebook.exit(f"Completed with {failed_outputs} failures.") # To make it a failed job

dbutils.notebook.exit(f"Data output processing finished. Successful: {successful_outputs}, Failed: {failed_outputs}.")
