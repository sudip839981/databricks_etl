# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer Processing Shell
# MAGIC
# MAGIC This notebook serves as a shell or template for creating Gold layer tables.
# MAGIC Gold layer tables typically involve aggregations, joins between Silver datasets, and application of specific business rules
# MAGIC to create data models suitable for reporting and analytics.
# MAGIC
# MAGIC **Process:**
# MAGIC 1. Reads configuration for the target Gold dataset.
# MAGIC 2. Loads required Silver layer datasets.
# MAGIC 3. **(USER ACTION REQUIRED)** Users will insert their custom PySpark/SQL logic for transformations in the designated section.
# MAGIC 4. Adds Gold layer audit columns.
# MAGIC 5. Writes the final Gold DataFrame to its configured table in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("dataset_id", "", "DatasetID from config.Datasets for the Gold dataset being produced")
dbutils.widgets.text("config_catalog", "main", "Unity Catalog for configuration tables")
dbutils.widgets.text("config_schema", "config", "Schema for configuration tables")
dbutils.widgets.text("silver_catalog", "silver_data", "Unity Catalog of the source Silver tables")
dbutils.widgets.text("silver_schema", "curated", "Schema of the source Silver tables")
dbutils.widgets.text("gold_catalog", "gold_data", "Target Unity Catalog for Gold layer tables")
dbutils.widgets.text("gold_schema", "presentation", "Target Schema for Gold layer tables")

# Retrieve widget values
dataset_id = dbutils.widgets.get("dataset_id")
config_catalog = dbutils.widgets.get("config_catalog")
config_schema = dbutils.widgets.get("config_schema")
silver_catalog = dbutils.widgets.get("silver_catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_catalog = dbutils.widgets.get("gold_catalog")
gold_schema = dbutils.widgets.get("gold_schema")

# Construct full paths for config tables
CONFIG_DATASETS_TABLE = f"{config_catalog}.{config_schema}.Datasets"
# ColumnMetadata might be used to define the schema of the Gold table, but transformations are custom.

print(f"Starting Gold processing for DatasetID (target Gold dataset): {dataset_id}")
print(f"Config Table: {CONFIG_DATASETS_TABLE}")
print(f"Source Silver Location: {silver_catalog}.{silver_schema}")
print(f"Target Gold Location: {gold_catalog}.{gold_schema}")

# Basic validation
if not dataset_id:
    dbutils.notebook.exit("ERROR: dataset_id widget (for the target Gold dataset) must be provided.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Configuration for Target Gold Dataset

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, expr
import json

try:
    print(f"Reading Dataset configuration for Gold DatasetID: {dataset_id} from {CONFIG_DATASETS_TABLE}")
    dataset_config_df = spark.read.table(CONFIG_DATASETS_TABLE).where(col("DatasetID") == dataset_id)
    target_gold_dataset_config = dataset_config_df.first()

    if not target_gold_dataset_config:
        dbutils.notebook.exit(f"ERROR: No configuration found for Gold DatasetID '{dataset_id}' in {CONFIG_DATASETS_TABLE}.")

    gold_table_name_from_config = target_gold_dataset_config.GoldLayerTargetTableName
    if not gold_table_name_from_config:
        dbutils.notebook.exit(f"ERROR: GoldLayerTargetTableName not configured for Gold DatasetID '{dataset_id}'.")

    print(f"Target Gold Dataset: {target_gold_dataset_config.DatasetName}, Target Table: {gold_table_name_from_config}")

    # Configuration for source Silver datasets
    # This needs a new field in 'Datasets' table for Gold datasets, e.g., 'SourceSilverDatasetIDs' (JSON array of DatasetIDs)
    # or 'GoldProcessingConfig' (JSON object with list of silver sources and their aliases)
    # For now, assume this configuration is part of the custom logic or defined by convention.
    # Example: source_silver_dataset_ids_json = target_gold_dataset_config.SourceSilverDatasetIDs
    # source_silver_dataset_ids = json.loads(source_silver_dataset_ids_json) if source_silver_dataset_ids_json else []

except Exception as e:
    print(f"ERROR: Failed to read configuration for Gold DatasetID '{dataset_id}': {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver Data
# MAGIC
# MAGIC Load the Silver layer datasets required for this Gold table.
# MAGIC This section will likely need to be customized by the user to specify which Silver tables to load
# MAGIC and assign them to DataFrame variables.

# COMMAND ----------

# Placeholder for loading Silver data. User will customize this.
# Example:
# silver_dfs = {}
# required_silver_sources = {
#    "sales_transactions": "id_of_sales_silver_dataset", # This mapping would come from config or be hardcoded per Gold notebook
#    "customer_dimension": "id_of_customer_silver_dataset"
# }

# for df_alias, silver_ds_id in required_silver_sources.items():
#    silver_ds_config_row = spark.read.table(CONFIG_DATASETS_TABLE).where(col("DatasetID") == silver_ds_id).first()
#    if silver_ds_config_row and silver_ds_config_row.SilverLayerTargetTableName:
#        table_name = silver_ds_config_row.SilverLayerTargetTableName
#        full_silver_path = f"{silver_catalog}.{silver_schema}.{table_name}"
#        print(f"Loading Silver dataset '{silver_ds_config_row.DatasetName}' (alias: {df_alias}) from {full_silver_path}")
#        silver_dfs[df_alias] = spark.read.table(full_silver_path)
#        silver_dfs[df_alias].printSchema() # Show schema for user context
#    else:
#        print(f"WARNING: Configuration or SilverLayerTargetTableName not found for Silver DatasetID '{silver_ds_id}'. Cannot load '{df_alias}'.")

# Example usage:
# sales_df = silver_dfs.get("sales_transactions")
# customer_df = silver_dfs.get("customer_dimension")

# if sales_df is None or customer_df is None:
#    dbutils.notebook.exit("ERROR: Could not load all required Silver datasets for Gold processing.")

print("---- Placeholder: Load Silver datasets as per specific Gold table requirements ----")
# Example:
# fact_orders_silver_df = spark.read.table(f"{silver_catalog}.{silver_schema}.fact_orders_silver")
# dim_products_silver_df = spark.read.table(f"{silver_catalog}.{silver_schema}.dim_products_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. USER ACTION REQUIRED: Implement Custom Gold Layer Logic
# MAGIC
# MAGIC Insert your PySpark or SQL code here to perform aggregations, joins, calculations, etc.,
# MAGIC to produce the final Gold DataFrame.
# MAGIC
# MAGIC Assign your final transformed DataFrame to a variable named `gold_df`.

# COMMAND ----------

# Example (replace with actual logic):
# gold_df = sales_df.join(customer_df, "customer_id", "inner") \
#                   .groupBy("customer_name", "product_category") \
#                   .agg(sum("amount").alias("total_sales"), count("*").alias("order_count"))

# Ensure gold_df is created by your logic.
# For this shell, create a simple passthrough or empty DF if required silver DFs are not loaded.
# This part MUST be customized by the user.

# If using the example silver_dfs loading above:
# if 'sales_df' in locals() and sales_df is not None:
#    gold_df = sales_df # Simple passthrough for shell
# else:
#    print("ERROR: Required Silver DataFrames not loaded. Cannot proceed with Gold logic.")
#    # Create an empty gold_df with a dummy schema or exit
#    # For shell purposes, let's create a dummy if it doesn't exist.
#    # This should be removed by the user when implementing their logic.
#    from pyspark.sql.types import StructType, StructField, StringType
#    dummy_schema = StructType([StructField("message", StringType(), True)])
#    gold_df = spark.createDataFrame([("Please implement Gold logic and define gold_df",)], schema=dummy_schema)


# Directly use example DFs for the shell to be runnable without complex setup
# User must replace this section.
print("---- EXECUTING DUMMY GOLD LOGIC - USER MUST REPLACE THIS ----")
try:
    # Attempt to create dummy silver DFs for shell execution if they don't exist
    if 'fact_orders_silver_df' not in locals():
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        orders_schema = StructType([
            StructField("OrderID", IntegerType()), StructField("ProductID", IntegerType()),
            StructField("CustomerID", IntegerType()), StructField("Quantity", IntegerType()), StructField("Price", DoubleType())])
        fact_orders_silver_df = spark.createDataFrame([(1,101,1,2,10.0),(2,102,1,1,20.0)], schema=orders_schema)
        print("Created dummy fact_orders_silver_df")

    if 'dim_products_silver_df' not in locals():
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        products_schema = StructType([StructField("ProductID", IntegerType()), StructField("ProductName", StringType())])
        dim_products_silver_df = spark.createDataFrame([(101,"ProductA"),(102,"ProductB")], schema=products_schema)
        print("Created dummy dim_products_silver_df")

    gold_df = fact_orders_silver_df.join(dim_products_silver_df, "ProductID") \
        .select("OrderID", "ProductName", "Quantity", "Price")

    print("Dummy Gold logic executed. Resulting gold_df schema:")
    gold_df.printSchema()

except Exception as e_dummy_gold:
    print(f"ERROR in dummy Gold logic placeholder: {e_dummy_gold}")
    from pyspark.sql.types import StructType, StructField, StringType
    dummy_schema = StructType([StructField("error_message", StringType(), True)])
    gold_df = spark.createDataFrame([(f"Error in Gold logic placeholder: {e_dummy_gold}",)], schema=dummy_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add Audit Columns

# COMMAND ----------

if 'gold_df' not in locals() or gold_df is None:
    dbutils.notebook.exit("ERROR: gold_df is not defined after custom Gold logic section. Please ensure your logic assigns to gold_df.")

gold_df_final = gold_df.withColumn("_gold_load_timestamp", current_timestamp())

print("Gold audit columns added.")
gold_df_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Gold Layer

# COMMAND ----------

full_gold_table_path = f"{gold_catalog}.{gold_schema}.{gold_table_name_from_config}"
print(f"Writing data to Gold table: {full_gold_table_path}")

try:
    # Ensure Gold catalog and schema exist (ideally pre-created)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {gold_catalog}")
    spark.sql(f"USE CATALOG {gold_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

    gold_df_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_gold_table_path)

    print(f"Successfully wrote data to {full_gold_table_path}")

except Exception as e:
    print(f"ERROR: Failed to write data to Gold table {full_gold_table_path}: {e}")
    raise

# COMMAND ----------

dbutils.notebook.exit(f"Successfully completed Gold processing for DatasetID: {dataset_id} into table {gold_table_name_from_config}")
