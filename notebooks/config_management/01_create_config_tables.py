# Databricks notebook source

# MAGIC %md
# MAGIC # Configuration Table Initialization
# MAGIC
# MAGIC This notebook creates the core configuration tables for the ETL framework in the specified Unity Catalog schema.
# MAGIC It reads DDL statements from the `docs/schema/configuration_tables_schema.md` file.
# MAGIC
# MAGIC **Important:**
# MAGIC * Run this notebook once to set up the tables.
# MAGIC * Ensure the target catalog and schema exist or you have permissions to create them if they don't (though schema creation is typically done manually beforehand).
# MAGIC * The user/principal running this notebook must have `CREATE TABLE` permissions on the target schema and `USAGE` permission on the catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Parameters

# COMMAND ----------

dbutils.widgets.text("target_catalog", "main", "Target Unity Catalog Name")
dbutils.widgets.text("target_schema", "config", "Target Schema Name within the Catalog")
# Path is relative to the notebook's location in notebooks/config_management/
# Assumes a Databricks Repo structure: <RepoRoot>/notebooks/config_management/ and <RepoRoot>/docs/schema/
dbutils.widgets.text("ddl_file_path", "../../../docs/schema/configuration_tables_schema.md", "Path to DDL Markdown File")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
ddl_file_path = dbutils.widgets.get("ddl_file_path")

print(f"Target Catalog: {target_catalog}")
print(f"Target Schema: {target_schema}")
print(f"DDL File Path (from widget): {ddl_file_path}")

# Construct the full schema name
full_schema_name = f"{target_catalog}.{target_schema}"
print(f"Full target schema path: {full_schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema (Recommended to do this manually)
# MAGIC
# MAGIC This notebook assumes the catalog and schema specified in the widgets already exist.
# MAGIC To create a schema, you can run SQL like:
# MAGIC `CREATE SCHEMA IF NOT EXISTS main.config COMMENT 'Schema for ETL framework configuration tables';`
# MAGIC Ensure the user running this notebook has the necessary privileges on `{full_schema_name}`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read DDL Statements from Markdown File

# COMMAND ----------

import re
import os

def read_ddl_from_markdown(relative_file_path):
    '''
    Reads SQL DDL statements embedded in a markdown file.
    Assumes DDL statements are enclosed in ```sql ... ``` blocks.
    The path is relative to the project root if using Databricks Repos.
    '''
    ddl_statements = []

    # In Databricks Repos, relative paths are typically from the Repo root.
    # The notebook is at <RepoRoot>/notebooks/config_management/01_create_config_tables.py
    # The DDL file is at <RepoRoot>/docs/schema/configuration_tables_schema.md
    # So, the path from Repo root is "docs/schema/configuration_tables_schema.md"
    # The widget default "../../../docs/schema/configuration_tables_schema.md" is relative to the notebook's *own* directory.

    # Let's try to construct a path relative to the current working directory,
    # which in Databricks Repos is usually the root of the repo.

    # Path provided by the widget is assumed to be relative from the notebook's directory.
    # For Databricks, the notebook's current directory is often the repo root when using Repos.
    # Let's try to be flexible.

    # Path to the notebook itself (this is a common way to get it, but might not always work in all exec contexts)
    # notebook_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    # absolute_ddl_file_path = os.path.join(notebook_dir, relative_file_path)

    # Given the widget default `../../../docs/schema/configuration_tables_schema.md`
    # and notebook location `notebooks/config_management/01_create_config_tables.py`
    # this path correctly points to `<RepoRoot>/docs/schema/configuration_tables_schema.md`

    # The ddl_file_path from the widget is what we'll use.
    # It's expected to be a path relative from the notebook's execution location.

    print(f"Attempting to read DDL file from resolved path: {relative_file_path}")
    print(f"Current working directory: {os.getcwd()}")


    try:
        with open(relative_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        sql_blocks = re.findall(r"```sql
(.*?)
```", content, re.DOTALL)

        for block in sql_blocks:
            block_no_comments = re.sub(r"--[^
]*", "", block) # Remove single line comments
            block_no_comments = re.sub(r"/\*.*?\*/", "", block_no_comments, flags=re.DOTALL) # Remove multi-line comments

            statements = [s.strip() for s in block_no_comments.split(';') if s.strip()]
            ddl_statements.extend(statements)

    except FileNotFoundError:
        print(f"ERROR: DDL file not found at '{relative_file_path}'. CWD: {os.getcwd()}")
        dbutils.notebook.exit(f"DDL file not found: {relative_file_path}. Check 'ddl_file_path' widget and ensure it's relative to the notebook's location or an absolute path accessible by the cluster.")
    except Exception as e:
        print(f"ERROR: Could not read or parse DDL statements from {relative_file_path}: {e}")
        raise

    return ddl_statements

# Use the path from the widget directly.
ddl_commands = read_ddl_from_markdown(ddl_file_path)

if ddl_commands:
    print(f"Successfully read {len(ddl_commands)} DDL statements.")
else:
    print(f"No DDL statements found in {ddl_file_path}. Please check the DDL file and path.")
    dbutils.notebook.exit("No DDL statements found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute DDL Statements
# MAGIC
# MAGIC This will now attempt to create each table within the `{full_schema_name}` schema.

# COMMAND ----------

failed_statements = []
success_count = 0

# Explicitly set the current catalog and schema for the Spark session.
# This is more reliable than relying on USE CATALOG/SCHEMA for DDLs if they are not fully qualified.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"USE CATALOG {target_catalog}")
print(f"Ensured catalog '{target_catalog}' exists and is selected.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name} COMMENT 'Schema for ETL framework configuration tables'")
spark.sql(f"USE SCHEMA {target_schema}") # target_schema is just the schema name, not fully qualified
print(f"Ensured schema '{full_schema_name}' exists and is selected.")


for ddl_original in ddl_commands:
    # The DDLs from markdown are like `CREATE TABLE IF NOT EXISTS config.TableName ...`
    # We need to replace `config.` with `full_schema_name.` to ensure they target the correct location,
    # or ensure the `config` part is removed if `USE SCHEMA` is effective for DDL.
    # Given we `USE CATALOG {target_catalog}` and `USE SCHEMA {target_schema}`,
    # DDLs like `CREATE TABLE IF NOT EXISTS TableName` (without schema qualifier) should work.
    # Let's adapt the DDLs to be schema-agnostic if `USE SCHEMA` is working,
    # or prefix them if not.
    # The DDLs are written as `config.TableName`. We will replace `config.` with `target_schema.`
    # and since `target_catalog` is already in `USE`, this should resolve correctly.

    # More robust: replace `config.` with `target_catalog.target_schema.`
    ddl_to_execute = ddl_original.replace("config.", f"{full_schema_name}.")

    ddl_to_execute = re.sub(r"--[^
]*", "", ddl_to_execute) # Re-clean comments just in case
    ddl_to_execute = ddl_to_execute.strip()

    if not ddl_to_execute:
        continue

    print(f"Attempting to execute:
{ddl_to_execute[:400]}...
")
    try:
        spark.sql(ddl_to_execute)
        print("SUCCESS.
")
        success_count += 1
    except Exception as e:
        error_message = str(e)
        print(f"ERROR executing DDL: {ddl_to_execute}
{error_message}
")
        failed_statements.append({"statement": ddl_to_execute, "error": error_message})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Total DDL statements found: {len(ddl_commands)}")
print(f"Successfully executed statements: {success_count}")
print(f"Failed statements: {len(failed_statements)}")

if failed_statements:
    print("\n--- Failed Statements ---")
    for i, failed_stmt in enumerate(failed_statements):
        print(f"Failure {i+1}:")
        print(f"  Statement: {failed_stmt['statement']}")
        print(f"  Error: {failed_stmt['error']}\n")
    print("WARNING: Some DDL statements failed. Check logs above. Ensure the schema was empty or DDLs are truly idempotent.")
else:
    print("\nAll DDL statements executed successfully.")
    print(f"Configuration tables should now be available under: {full_schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next Steps:
# MAGIC * Manually verify the tables in Unity Catalog under the schema: `{full_schema_name}`.
# MAGIC * Proceed to populate the tables with initial configuration data using other notebooks.
