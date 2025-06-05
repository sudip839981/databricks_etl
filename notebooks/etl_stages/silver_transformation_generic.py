# Databricks notebook source

# MAGIC %md
# MAGIC # Generic Silver Layer Transformation
# MAGIC
# MAGIC This notebook transforms data from a Bronze layer table to a Silver layer table.
# MAGIC It is driven by metadata stored in the `Datasets`, `ColumnMetadata`, and `DQMRules` configuration tables.
# MAGIC
# MAGIC **Responsibilities:**
# MAGIC 1. Read Bronze data.
# MAGIC 2. Read transformation and DQM rules from `ColumnMetadata` and `DQMRules`.
# MAGIC 3. Apply transformations: renaming, data type casting, basic cleaning logic.
# MAGIC 4. Apply Data Quality Management (DQM) checks.
# MAGIC 5. Quarantine records failing critical DQM checks (optional, configurable).
# MAGIC 6. Add Silver layer audit columns.
# MAGIC 7. Write the cleansed, conformed data to the Silver layer Delta table in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("dataset_id", "", "DatasetID from config.Datasets to process")
dbutils.widgets.text("config_catalog", "main", "Unity Catalog for configuration tables")
dbutils.widgets.text("config_schema", "config", "Schema for configuration tables")
dbutils.widgets.text("bronze_catalog", "bronze_data", "Unity Catalog of the source Bronze table")
dbutils.widgets.text("bronze_schema", "landing", "Schema of the source Bronze table")
dbutils.widgets.text("silver_catalog", "silver_data", "Target Unity Catalog for Silver layer tables")
dbutils.widgets.text("silver_schema", "curated", "Target Schema for Silver layer tables")

# Retrieve widget values
dataset_id = dbutils.widgets.get("dataset_id")
config_catalog = dbutils.widgets.get("config_catalog")
config_schema = dbutils.widgets.get("config_schema")
bronze_catalog = dbutils.widgets.get("bronze_catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_catalog = dbutils.widgets.get("silver_catalog")
silver_schema = dbutils.widgets.get("silver_schema")

# Construct full paths for config tables
CONFIG_DATASETS_TABLE = f"{config_catalog}.{config_schema}.Datasets"
CONFIG_COLUMNMETADATA_TABLE = f"{config_catalog}.{config_schema}.ColumnMetadata"
CONFIG_DQMRULES_TABLE = f"{config_catalog}.{config_schema}.DQMRules"

print(f"Starting Silver transformation for DatasetID: {dataset_id}")
print(f"Config Tables: {CONFIG_DATASETS_TABLE}, {CONFIG_COLUMNMETADATA_TABLE}, {CONFIG_DQMRULES_TABLE}")
print(f"Source Bronze Location: {bronze_catalog}.{bronze_schema}")
print(f"Target Silver Location: {silver_catalog}.{silver_schema}")

# Basic validation
if not dataset_id:
    dbutils.notebook.exit("ERROR: dataset_id widget must be provided.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Configuration
# MAGIC
# MAGIC Fetch metadata for the given `dataset_id` from `Datasets`, `ColumnMetadata`.

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, expr
from pyspark.sql.types import StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, DateType, TimestampType, DecimalType, ArrayType, MapType, StructType

def get_spark_data_type(type_string):
    type_string_lower = type_string.lower()
    if type_string_lower == "string": return StringType()
    elif type_string_lower == "integer" or type_string_lower == "int": return IntegerType()
    elif type_string_lower == "long": return LongType()
    elif type_string_lower == "float": return FloatType()
    elif type_string_lower == "double": return DoubleType()
    elif type_string_lower == "boolean": return BooleanType()
    elif type_string_lower == "date": return DateType()
    elif type_string_lower == "timestamp": return TimestampType()
    elif type_string_lower.startswith("decimal"): # Handles "decimal(p,s)"
        try:
            precision, scale = map(int, type_string_lower.replace("decimal(", "").replace(")", "").split(','))
            return DecimalType(precision, scale)
        except ValueError:
            print(f"WARNING: Invalid Decimal format '{type_string}'. Using StringType as fallback.")
            return StringType() # Fallback or raise error
    # Add more complex types like array, map, struct if needed based on your config values
    # elif type_string_lower.startswith("array<"): # e.g. array<string>
    #    element_type_str = type_string_lower.replace("array<", "")[:-1]
    #    return ArrayType(get_spark_data_type(element_type_str))
    else:
        print(f"WARNING: Unsupported data type string '{type_string}'. Using StringType as fallback.")
        return StringType() # Default fallback

try:
    print(f"Reading Dataset configuration for DatasetID: {dataset_id} from {CONFIG_DATASETS_TABLE}")
    dataset_config_df = spark.read.table(CONFIG_DATASETS_TABLE).where(col("DatasetID") == dataset_id)
    dataset_config = dataset_config_df.first()

    if not dataset_config:
        dbutils.notebook.exit(f"ERROR: No configuration found for DatasetID '{dataset_id}' in {CONFIG_DATASETS_TABLE}.")

    print(f"Dataset configuration found: {dataset_config.DatasetName}")
    bronze_table_name_from_config = dataset_config.BronzeLayerTargetTableName # From previous correction
    silver_table_name_from_config = dataset_config.SilverLayerTargetTableName

    if not bronze_table_name_from_config:
        # Fallback logic similar to bronze ingestion if BronzeLayerTargetTableName might be empty
        bronze_table_name_from_config = f"brz_{dataset_config.DatasetName.lower().replace(' ', '_').replace('-', '_')}"
        print(f"WARNING: BronzeLayerTargetTableName not found in Datasets config for {dataset_id}, derived as {bronze_table_name_from_config}")

    if not silver_table_name_from_config:
        dbutils.notebook.exit(f"ERROR: SilverLayerTargetTableName not configured for DatasetID '{dataset_id}' in {CONFIG_DATASETS_TABLE}.")

    # Read ColumnMetadata for this dataset
    print(f"Reading ColumnMetadata for DatasetID: {dataset_id} from {CONFIG_COLUMNMETADATA_TABLE}")
    column_metadata_df = spark.read.table(CONFIG_COLUMNMETADATA_TABLE) \
        .where((col("DatasetID") == dataset_id) & (col("IsEnabled") == True)) \
        .orderBy("TargetOrdinalPosition") # Process columns in specified target order if defined

    column_configs = column_metadata_df.collect()
    if not column_configs:
        dbutils.notebook.exit(f"ERROR: No enabled ColumnMetadata found for DatasetID '{dataset_id}' in {CONFIG_COLUMNMETADATA_TABLE}.")

    print(f"Found {len(column_configs)} column configurations for {dataset_id}.")

    # Pre-fetch all unique DQM rule definitions
    all_dqm_rule_ids = set()
    for config_row in column_configs: # Iterate over Row objects
        if config_row.DQMRuleIDs: # DQMRuleIDs is an ARRAY<STRING>
            for rule_id in config_row.DQMRuleIDs:
                all_dqm_rule_ids.add(rule_id)

    dqm_rules_definitions = {}
    if all_dqm_rule_ids:
        print(f"Fetching definitions for DQM Rule IDs: {list(all_dqm_rule_ids)}")
        dqm_rules_df = spark.read.table(CONFIG_DQMRULES_TABLE).where(col("DQMRuleID").isin(list(all_dqm_rule_ids)))
        for rule_row_def in dqm_rules_df.collect(): # Rename to avoid conflict with config_row
            dqm_rules_definitions[rule_row_def.DQMRuleID] = rule_row_def
        print(f"Fetched {len(dqm_rules_definitions)} DQM rule definitions.")
    else:
        print("No DQM Rule IDs found in ColumnMetadata for this dataset.")

except Exception as e:
    print(f"ERROR: Failed to read configuration: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze Data

# COMMAND ----------

full_bronze_table_path = f"{bronze_catalog}.{bronze_schema}.{bronze_table_name_from_config}"
print(f"Reading Bronze data from: {full_bronze_table_path}")

try:
    bronze_df = spark.read.table(full_bronze_table_path)
    print(f"Successfully read Bronze data. Schema:")
    bronze_df.printSchema()
    # bronze_df.show(5, truncate=False) # For debugging
except Exception as e:
    print(f"ERROR: Failed to read Bronze data from {full_bronze_table_path}. Error: {e}")
    raise

if bronze_df.isEmpty():
    print(f"WARNING: Bronze table {full_bronze_table_path} is empty. Silver processing will result in an empty table.")
    # No need to exit, allow empty table processing.
    pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply Transformations based on ColumnMetadata
# MAGIC
# MAGIC Includes renaming, data type casting, and other defined transformations.

# COMMAND ----------

from pyspark.sql.functions import col, expr # Ensure expr is imported if not already

if bronze_df.isEmpty():
    print("Bronze DataFrame is empty, skipping transformations. Output Silver table will also be empty.")
    # If bronze_df is empty, transformed_df should also be empty but with the target schema.
    # Create an empty DataFrame with the target schema based on column_configs
    from pyspark.sql.types import StructField
    silver_fields = []
    for config in column_configs:
        target_col_name = config.TargetColumnName
        spark_type = get_spark_data_type(config.DataType) if config.DataType else StringType() # Default to String if no type
        is_nullable = config.IsNullable if config.IsNullable is not None else True # Default to nullable
        silver_fields.append(StructField(target_col_name, spark_type, is_nullable))

    if silver_fields:
        empty_silver_schema = StructType(silver_fields)
        transformed_df = spark.createDataFrame([], schema=empty_silver_schema)
    else: # Should not happen if column_configs is populated
        transformed_df = bronze_df
else:
    select_expressions = []
    for config in column_configs:
        source_col_name = config.SourceColumnName
        target_col_name = config.TargetColumnName
        target_data_type_str = config.DataType
        transformation_logic_str = config.TransformationLogic

        # Start with selecting the source column
        if source_col_name not in bronze_df.columns:
            print(f"WARNING: Source column '{source_col_name}' defined in ColumnMetadata for target '{target_col_name}' not found in Bronze DataFrame. Skipping this column.")
            continue # Skip this column config

        current_col_expr = col(source_col_name)

        # 1. Apply TransformationLogic if defined
        # This version assumes TransformationLogic is a Spark SQL expression string
        # that refers to the source column by its actual name (source_col_name).
        # Example: TransformationLogic = "TRIM(source_column_name)"
        # Example: TransformationLogic = "CASE WHEN source_column_name > 0 THEN 'Positive' ELSE 'Negative' END"
        # Example: TransformationLogic = "CAST(source_column_name AS INT) * 10" (casting handled separately, but shows flexibility)
        if transformation_logic_str:
            try:
                # A common placeholder for the column itself in the expression string could be '{{this}}' or source_col_name itself
                # For simplicity, assume the expression directly uses the source_col_name or is self-contained.
                # If using a placeholder like '{{this}}':
                # current_col_expr = expr(transformation_logic_str.replace("{{this}}", source_col_name))
                # If the expression is like "TRIM({{this}})" or "upper({{this}})"
                # A more robust way: check if transformation_logic_str is a known function name or a complex expression.
                # For now, directly use expr()
                print(f"Applying transformation expression for {source_col_name}: {transformation_logic_str}")
                current_col_expr = expr(transformation_logic_str) # Assumes source_col_name is part of the expression string
            except Exception as e_expr:
                print(f"WARNING: Could not apply TransformationLogic '{transformation_logic_str}' for source column '{source_col_name}'. Error: {e_expr}. Using original column value.")
                current_col_expr = col(source_col_name) # Fallback to original column if expression fails

        # 2. Apply Data Type Casting
        if target_data_type_str:
            spark_type = get_spark_data_type(target_data_type_str)
            print(f"Casting {target_col_name} (from {source_col_name}) to {str(spark_type)}")
            current_col_expr = current_col_expr.cast(spark_type)

        # 3. Alias to Target Column Name
        select_expressions.append(current_col_expr.alias(target_col_name))

    if not select_expressions:
        if not column_configs: # This case should be caught earlier when column_configs is populated
             dbutils.notebook.exit("ERROR: No column configurations available to build Silver table schema.")
        else: # This means all source columns were missing from Bronze DF
            print("WARNING: No columns to select after processing ColumnMetadata (all source columns might be missing from Bronze DF). Resulting Silver DF will be empty or schema-only.")
            # Create an empty DataFrame with the target schema
            from pyspark.sql.types import StructField
            silver_fields = []
            for config in column_configs:
                target_col_name = config.TargetColumnName
                spark_type = get_spark_data_type(config.DataType) if config.DataType else StringType()
                is_nullable = config.IsNullable if config.IsNullable is not None else True
                silver_fields.append(StructField(target_col_name, spark_type, is_nullable))
            if silver_fields:
                empty_silver_schema = StructType(silver_fields)
                transformed_df = spark.createDataFrame([], schema=empty_silver_schema)
            else: # Should not happen
                transformed_df = spark.createDataFrame([], schema=bronze_df.schema) # Fallback, likely problematic

    else:
        transformed_df = bronze_df.select(*select_expressions)

print("Transformations applied. Resulting schema:")
transformed_df.printSchema()
# transformed_df.show(5, truncate=False) # For debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply DQM Checks based on DQMRules

# COMMAND ----------

import json # For parsing RuleConfiguration

if bronze_df.isEmpty(): # If source was empty, transformed_df is also empty with target schema
    print("Transformed DataFrame is empty, skipping DQM checks.")
    dqm_passed_df = transformed_df.withColumn("_row_dqm_failed", lit(False).cast("boolean")) # Ensure column exists even for empty
    # dqm_failed_records_details = [] # No DQM failures # Not used in this version
elif not dqm_rules_definitions: # No rules to apply
    print("No DQM Rule definitions were loaded. Skipping DQM checks.")
    dqm_passed_df = transformed_df.withColumn("_row_dqm_failed", lit(False).cast("boolean"))
    # dqm_failed_records_details = [] # No DQM failures # Not used in this version
else:
    print("Applying DQM checks...")
    # Add a temporary unique ID for each row to track failures if we split rows later
    df_with_id = transformed_df.withColumn("_temp_row_id", expr("monotonically_increasing_id()"))

    dqm_failure_flags = [] # List to store expressions for failure flags

    for config in column_configs:
        target_col_name = config.TargetColumnName # DQM is applied on the transformed column name
        if not config.DQMRuleIDs:
            continue

        if target_col_name not in df_with_id.columns:
            print(f"WARNING: Column '{target_col_name}' for DQM not found in DataFrame. Skipping DQM for this column.")
            continue

        for rule_id in config.DQMRuleIDs:
            rule_def = dqm_rules_definitions.get(rule_id)
            if not rule_def or not rule_def.IsEnabled:
                print(f"RuleID {rule_id} not found or not enabled. Skipping.")
                continue

            rule_type = rule_def.RuleType
            rule_config_json = rule_def.RuleConfiguration
            rule_name = rule_def.RuleName
            dqm_flag_col_name = f"_dqm_{target_col_name}_{rule_name}_failed"

            try:
                rule_params = json.loads(rule_config_json) if rule_config_json else {}
            except json.JSONDecodeError:
                print(f"WARNING: Invalid JSON in RuleConfiguration for RuleID {rule_id} ('{rule_name}'). Skipping this rule.")
                continue

            print(f"Applying DQM Rule '{rule_name}' (Type: {rule_type}) on column '{target_col_name}'")

            failure_condition = None
            if rule_type.upper() == "NOT_NULL":
                failure_condition = col(target_col_name).isNull()

            elif rule_type.upper() == "REGEX":
                pattern = rule_params.get("pattern")
                if pattern:
                    # Ensure column is string type for rlike, or handle potential type errors
                    # rlike on null is null. If nulls should fail regex, this is implicitly handled as not matching.
                    failure_condition = ~col(target_col_name).cast("string").rlike(pattern)
                else:
                    print(f"WARNING: Pattern not found in RuleConfiguration for REGEX rule '{rule_name}'. Skipping.")

            elif rule_type.upper() == "RANGE":
                min_value = rule_params.get("min_value")
                max_value = rule_params.get("max_value")
                # Values in rule_params might be strings, ensure they are cast to the column's type for comparison
                # This requires knowing the column's type. For simplicity, assume direct comparison works or types are compatible.
                # A more robust way would be to cast min/max_value to the type of target_col_name.

                # Check if column is null - range checks typically don't apply to nulls unless specified
                # If nulls should fail a range check, it should be a separate NOT_NULL check.
                # So, failure is when (value < min OR value > max) AND value IS NOT NULL

                col_is_not_null = col(target_col_name).isNotNull()

                if min_value is not None and max_value is not None:
                    failure_condition = col_is_not_null & ((col(target_col_name) < lit(min_value)) | (col(target_col_name) > lit(max_value)))
                elif min_value is not None: # Only min specified
                    failure_condition = col_is_not_null & (col(target_col_name) < lit(min_value))
                elif max_value is not None: # Only max specified
                    failure_condition = col_is_not_null & (col(target_col_name) > lit(max_value))
                else:
                    print(f"WARNING: min_value and/or max_value not found in RuleConfiguration for RANGE rule '{rule_name}'. Skipping.")

            elif rule_type.upper() == "LENGTH":
                min_length = rule_params.get("min_length")
                max_length = rule_params.get("max_length")

                # LENGTH function in Spark SQL is for strings. Ensure column is string type.
                # If nulls should fail a length check, it should be a separate NOT_NULL check.
                # So, failure is when (length < min OR length > max) AND value IS NOT NULL

                col_is_not_null_for_length = col(target_col_name).isNotNull()
                # Calculate length of the string representation of the column
                # Using `length(trim(col(...)))` might be good to avoid issues with leading/trailing spaces if desired.
                # For now, direct length on casted string.
                actual_length = expr(f"LENGTH(CAST(`{target_col_name}` AS STRING))")

                if min_length is not None and max_length is not None:
                    failure_condition = col_is_not_null_for_length & ((actual_length < lit(min_length).cast("int")) | (actual_length > lit(max_length).cast("int")))
                elif min_length is not None:
                    failure_condition = col_is_not_null_for_length & (actual_length < lit(min_length).cast("int"))
                elif max_length is not None:
                    failure_condition = col_is_not_null_for_length & (actual_length > lit(max_length).cast("int"))
                else:
                    print(f"WARNING: min_length and/or max_length not found in RuleConfiguration for LENGTH rule '{rule_name}'. Skipping.")

            elif rule_type.upper() == "CUSTOM_SQL_EXPRESSION":
                custom_expression = rule_params.get("expression")
                if custom_expression:
                    # The custom_expression should evaluate to TRUE for a valid record.
                    # Therefore, the failure condition is when the expression is NOT TRUE.
                    # This handles cases where the expression might be NULL (e.g., due to a NULL in one of its inputs).
                    # NOT (NULL) is NULL. NULL cast to boolean for the flag column becomes NULL.
                    # The overall row_dqm_failed logic (`flag_col = true`) correctly treats NULL flags as not true.
                    try:
                        print(f"Applying CUSTOM_SQL_EXPRESSION for rule '{rule_name}': NOT ({custom_expression})")
                        failure_condition = ~(expr(custom_expression))
                    except Exception as e_cust_expr:
                        print(f"WARNING: Error evaluating CUSTOM_SQL_EXPRESSION '{custom_expression}' for rule '{rule_name}'. Error: {e_cust_expr}. Skipping this rule for the column.")
                        failure_condition = None # Skip if expression is invalid
                else:
                    print(f"WARNING: 'expression' not found in RuleConfiguration for CUSTOM_SQL_EXPRESSION rule '{rule_name}'. Skipping.")

            elif rule_type.upper() == "LOOKUP":
                lookup_values_list = rule_params.get("values") # List of allowed values
                lookup_table_name_str = rule_params.get("lookup_table") # e.g., "catalog.schema.table"
                lookup_column_name_str = rule_params.get("lookup_column") # column in lookup_table
                # Potential cache for lookup sets from tables: {(table, column): set_of_values}
                # This cache would ideally be defined outside the column/rule loop for broader reuse within the notebook run.
                # For now, not implementing caching in this subtask, will read each time.

                # Ensure custom_expression is defined for the subsequent elif checks, even if not used by LOOKUP itself.
                custom_expression = rule_params.get("expression") if rule_type.upper() == "CUSTOM_SQL_EXPRESSION" else None


                if lookup_values_list and isinstance(lookup_values_list, list):
                    print(f"Applying List-based LOOKUP for rule '{rule_name}' on column '{target_col_name}'.")
                    failure_condition = ~col(target_col_name).isin(lookup_values_list)
                elif lookup_table_name_str and lookup_column_name_str:
                    print(f"Applying Table-based LOOKUP for rule '{rule_name}' on column '{target_col_name}' using table '{lookup_table_name_str}.{lookup_column_name_str}'.")
                    try:
                        # Read distinct values from the lookup table.
                        # This could be inefficient if the same lookup table/column is used many times without caching.
                        print(f"Reading distinct values from lookup table: {lookup_table_name_str}, column: {lookup_column_name_str}")

                        # Check if lookup_table_name_str exists
                        try:
                            spark.read.table(lookup_table_name_str).limit(0) # Check existence
                        except Exception as e_table_exists:
                            print(f"ERROR: Lookup table '{lookup_table_name_str}' not found or accessible for rule '{rule_name}'. Error: {e_table_exists}. Skipping rule.")
                            failure_condition = None # Skip rule if table doesn't exist
                            # Need to 'continue' to the next rule_id within the inner loop, not just set failure_condition
                            # This 'continue' will skip the 'if failure_condition is not None' block for this rule_id
                            # and proceed to the next rule_id for the current column config.
                            # The structure is: for config in column_configs: for rule_id in config.DQMRuleIDs: ...
                            # So, 'continue' here skips the rest of the code for the current rule_id.
                            # The 'if failure_condition is not None' check later will handle this.
                            # However, if this is the *only* rule for a column, and it's skipped, the column won't get a DQM flag.
                            # The print statement is the primary action for a skipped rule due to bad config.
                            # The outer loop structure handles this correctly.
                            # Setting failure_condition = None and letting it fall through is the current pattern.
                            # If we 'continue' here, the final 'if failure_condition is not None:' won't be hit for this rule.
                            # This is acceptable as the rule effectively hasn't produced a condition.
                            # Let's ensure the final elif correctly identifies this skip.

                            # To ensure the skip is correctly handled and doesn't lead to a "not implemented" warning:
                            # Add this to make it explicit for the outer conditional checks
                            if 'failure_condition' not in locals() or failure_condition is not None: # if it was not set or set by previous rules
                                failure_condition = None # Explicitly state this rule instance results in no condition
                            # The 'continue' here is problematic as it would skip the 'if failure_condition is not None' check.
                            # Instead, let it flow, and the 'if failure_condition is not None' will simply not execute for this rule.
                            # The warning is printed, which is the main outcome of this path.

                        lookup_df = spark.read.table(lookup_table_name_str).select(lookup_column_name_str).distinct()

                        # Check if lookup_column_name_str exists in the lookup_df
                        if lookup_column_name_str not in lookup_df.columns:
                            print(f"ERROR: Lookup column '{lookup_column_name_str}' not found in lookup table '{lookup_table_name_str}' for rule '{rule_name}'. Skipping rule.")
                            failure_condition = None
                            # Similar to above, let this flow.

                        distinct_lookup_values = [row[0] for row in lookup_df.collect()]

                        if not distinct_lookup_values:
                            print(f"WARNING: Lookup table '{lookup_table_name_str}' column '{lookup_column_name_str}' for rule '{rule_name}' is empty or all values are null. This LOOKUP rule will cause all non-null records to fail.")
                            failure_condition = col(target_col_name).isNotNull()
                        else:
                            failure_condition = ~col(target_col_name).isin(distinct_lookup_values)
                    except Exception as e_lookup_table:
                        print(f"ERROR processing table-based LOOKUP for rule '{rule_name}' (table: {lookup_table_name_str}). Error: {e_lookup_table}. Skipping rule.")
                        failure_condition = None
                else:
                    print(f"WARNING: 'values' (as a list) or 'lookup_table'/'lookup_column' not found/valid in RuleConfiguration for LOOKUP rule '{rule_name}'. Skipping.")
                    failure_condition = None

            # Add more rule types here ...

            if failure_condition is not None:
                df_with_id = df_with_id.withColumn(dqm_flag_col_name, failure_condition.cast("boolean"))
                dqm_failure_flags.append(dqm_flag_col_name)
            # Adjusted elif conditions to properly catch skipped rules due to bad config vs. genuinely unimplemented types
            elif rule_type.upper() == "LOOKUP" and not (lookup_values_list or (lookup_table_name_str and lookup_column_name_str)):
                 pass # Already printed warning for bad LOOKUP config
            elif rule_type.upper() == "CUSTOM_SQL_EXPRESSION" and not custom_expression:
                 pass # Already printed warning for bad CUSTOM_SQL_EXPRESSION config
            # The case where failure_condition is None because a rule was skipped internally (e.g. table not found for lookup)
            # is handled by the fact that dqm_failure_flags won't get this rule's flag_col_name.
            # The final warning for genuinely unimplemented types:
            elif failure_condition is None and rule_type.upper() not in ["NOT_NULL", "REGEX", "RANGE", "LENGTH", "CUSTOM_SQL_EXPRESSION", "LOOKUP"]:
                 print(f"WARNING: RuleType '{rule_type}' for rule '{rule_name}' not implemented or condition not set. Skipping.")

    # Combine all DQM failure flags for a row: if any DQM check failed, mark the row.
    if dqm_failure_flags:
        overall_dqm_failed_expr_str = " OR ".join([f"`{flag_col}` = true" for flag_col in dqm_failure_flags])
        df_with_id = df_with_id.withColumn("_row_dqm_failed", expr(overall_dqm_failed_expr_str).cast("boolean"))

        failed_records_count = df_with_id.where(col("_row_dqm_failed") == True).count()
        print(f"DQM Check Summary: Found {failed_records_count} rows with at least one DQM failure.")
        # For debugging, you could show some failed records:
        # if failed_records_count > 0:
        #     print("Sample of failed records (showing temp_row_id and failure flags):")
        #     df_with_id.where(col("_row_dqm_failed") == True).select(
        #        col("_temp_row_id"),
        #        *[col(flag) for flag in dqm_failure_flags if flag in df_with_id.columns]
        #     ).show(5, truncate=False)


        # New DQM Quarantine Logic Starts
        if not dqm_failure_flags: # No DQM rules were actually applied or generated flags
            dqm_passed_df = transformed_df.withColumn("_row_dqm_failed", lit(False).cast("boolean"))
            if "_temp_row_id" in dqm_passed_df.columns: # df_with_id might be same as transformed_df if no rules
                 dqm_passed_df = dqm_passed_df.drop("_temp_row_id")
            print("No DQM rules applied or no failure flags generated.")
        else:
            critical_failure_row_ids = set()
            rows_to_quarantine_details = [] # List of dicts for creating quarantine DF

            # Collect rows that have at least one DQM flag set to true
            # This requires all flag columns to be present on df_with_id
            # Also select all original target column names for FailedValue logging
            relevant_target_cols_for_quarantine = [cc.TargetColumnName for cc in column_configs if cc.TargetColumnName in df_with_id.columns]

            # Constructing the filter expression for rows with any DQM failure
            any_dqm_failure_expr_str = " OR ".join([f"`{f}` = true" for f in dqm_failure_flags])

            # df_with_failures_collected = df_with_id.where(expr(any_dqm_failure_expr_str)).select("_temp_row_id", *dqm_failure_flags, *relevant_target_cols_for_quarantine).collect()
            # Collecting all rows with failures might be memory intensive if many rows fail.
            # Alternative: iterate and process, but that's more complex with Spark.
            # For now, proceed with collect, assuming failures are a manageable subset.
            # If this becomes a bottleneck, this part needs optimization (e.g. map operations, or more selective collection).

            # Let's refine to avoid collecting entire rows if possible, or limit what's collected.
            # The core idea is to identify rows with CRITICAL failures.

            print(f"Identifying critical DQM failures ('ERROR' severity)...")
            # Iterate through rules to build expressions for critical failures
            critical_failure_expressions = []
            for config in column_configs:
                target_col_name_for_quarantine = config.TargetColumnName
                if not config.DQMRuleIDs: continue
                if target_col_name_for_quarantine not in df_with_id.columns: continue

                for rule_id in config.DQMRuleIDs:
                    rule_def = dqm_rules_definitions.get(rule_id)
                    if not rule_def or not rule_def.IsEnabled: continue

                    if rule_def.SeverityLevel.upper() == "ERROR":
                        flag_name_to_check = f"_dqm_{target_col_name_for_quarantine}_{rule_def.RuleName}_failed"
                        if flag_name_to_check in dqm_failure_flags: # Ensure this flag was actually created
                             critical_failure_expressions.append(f"`{flag_name_to_check}` = true")

            if critical_failure_expressions:
                critical_rows_filter_expr = " OR ".join(critical_failure_expressions)
                critically_failed_rows_df = df_with_id.where(expr(critical_rows_filter_expr))

                # Now collect details for only these critically failed rows
                # This is still a collect, but potentially on fewer rows than "any failure"
                collected_critical_rows = critically_failed_rows_df.select("_temp_row_id", *dqm_failure_flags, *relevant_target_cols_for_quarantine).collect()

                for temp_row in collected_critical_rows:
                    original_row_id = temp_row["_temp_row_id"]
                    critical_failure_row_ids.add(original_row_id) # Mark this row ID for removal from main DF

                    # Identify which specific critical rules failed for this row
                    for config in column_configs:
                        target_col_name_for_quarantine = config.TargetColumnName
                        if not config.DQMRuleIDs: continue
                        if target_col_name_for_quarantine not in df_with_id.columns: continue # Should not happen if in relevant_target_cols_for_quarantine

                        for rule_id in config.DQMRuleIDs:
                            rule_def = dqm_rules_definitions.get(rule_id)
                            if not rule_def or not rule_def.IsEnabled: continue
                            if rule_def.SeverityLevel.upper() == "ERROR":
                                flag_name_to_check = f"_dqm_{target_col_name_for_quarantine}_{rule_def.RuleName}_failed"
                                if flag_name_to_check in temp_row and temp_row[flag_name_to_check] == True:
                                    rows_to_quarantine_details.append({
                                        "DatasetID": dataset_id, "OriginalRowID": original_row_id,
                                        "TargetColumnName": target_col_name_for_quarantine, "DQMRuleID": rule_id,
                                        "DQMRuleName": rule_def.RuleName, "DQMRuleType": rule_def.RuleType,
                                        "DQMRuleSeverity": rule_def.SeverityLevel,
                                        "FailedValue": str(temp_row[target_col_name_for_quarantine])[:250], # Truncate
                                        "DQMCheckTimestamp": current_timestamp() # Batch timestamp
                                    })
            else:
                print("No DQM rules with 'ERROR' severity found or applied.")


            if rows_to_quarantine_details:
                quarantine_df_schema = StructType([
                    StructField("DatasetID", StringType(), True), StructField("OriginalRowID", LongType(), True),
                    StructField("TargetColumnName", StringType(), True), StructField("DQMRuleID", StringType(), True),
                    StructField("DQMRuleName", StringType(), True), StructField("DQMRuleType", StringType(), True),
                    StructField("DQMRuleSeverity", StringType(), True), StructField("FailedValue", StringType(), True),
                    StructField("DQMCheckTimestamp", TimestampType(), True)
                ])
                quarantine_df = spark.createDataFrame(rows_to_quarantine_details, schema=quarantine_df_schema)

                quarantine_table_name_suffix = dataset_config.DatasetName.lower().replace(' ', '_').replace('-', '_')
                # Using silver_catalog and silver_schema for quarantine table for now
                quarantine_table_full_name = f"{silver_catalog}.{silver_schema}.svr_dqm_exceptions_{quarantine_table_name_suffix}"
                print(f"Writing {quarantine_df.count()} critical DQM failure details to: {quarantine_table_full_name}")
                try:
                    # Ensure quarantine schema exists
                    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}.{silver_schema}")
                    quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table_full_name)
                except Exception as e_q_write:
                    print(f"ERROR writing to DQM quarantine table {quarantine_table_full_name}: {e_q_write}. Critical failures might not be quarantined.")

            # Filter out rows with critical failures from the main DataFrame
            if critical_failure_row_ids:
                print(f"Quarantining {len(critical_failure_row_ids)} rows due to critical DQM errors.")
                dqm_passed_df_intermediate = df_with_id.where(~col("_temp_row_id").isin(list(critical_failure_row_ids)))
            else:
                print("No critical DQM failures found for quarantine.")
                dqm_passed_df_intermediate = df_with_id

            # Set overall DQM failed flag for remaining rows (those not quarantined)
            # This flag indicates if ANY rule failed for the row, critical or not.
            # Rows that were quarantined are no longer in dqm_passed_df_intermediate.
            if dqm_failure_flags: # Check if there were any flags to begin with
                overall_dqm_failed_expr_str_for_passed = " OR ".join([f"`{f}` = true" for f in dqm_failure_flags])
                dqm_passed_df_intermediate = dqm_passed_df_intermediate.withColumn("_row_dqm_failed", expr(overall_dqm_failed_expr_str_for_passed).cast("boolean"))
            else: # Should not happen if outer 'if not dqm_failure_flags:' is false, but as a safe guard
                dqm_passed_df_intermediate = dqm_passed_df_intermediate.withColumn("_row_dqm_failed", lit(False).cast("boolean"))


            # Drop temporary and individual flag columns
            columns_to_drop = [flag for flag in dqm_failure_flags if flag in dqm_passed_df_intermediate.columns]
            columns_to_drop.append("_temp_row_id")
            dqm_passed_df = dqm_passed_df_intermediate.drop(*columns_to_drop)
        # End of main else for 'if not dqm_failure_flags:'

print("DQM checks and quarantine logic applied. Resulting schema:")
dqm_passed_df.printSchema()
# dqm_passed_df.show(5, truncate=False) # For debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add Audit Columns

# COMMAND ----------

silver_df_final = dqm_passed_df.withColumn("_silver_load_timestamp", current_timestamp())
# Optionally add other audit columns like _data_quality_score if DQM is implemented

print("Silver audit columns added.")
silver_df_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Silver Layer

# COMMAND ----------

full_silver_table_path = f"{silver_catalog}.{silver_schema}.{silver_table_name_from_config}"
print(f"Writing data to Silver table: {full_silver_table_path}")

try:
    # Ensure Silver catalog and schema exist (ideally pre-created)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {silver_catalog}")
    spark.sql(f"USE CATALOG {silver_catalog}") # Context setting
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

    silver_df_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_silver_table_path)

    print(f"Successfully wrote data to {full_silver_table_path}")

except Exception as e:
    print(f"ERROR: Failed to write data to Silver table {full_silver_table_path}: {e}")
    raise

# COMMAND ----------

dbutils.notebook.exit(f"Successfully completed Silver transformation for DatasetID: {dataset_id}")
