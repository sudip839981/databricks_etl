# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks Workflow Generator
# MAGIC
# MAGIC This notebook reads pipeline configurations from the ETL framework's metadata tables
# MAGIC and generates the JSON definition for a Databricks Workflow.
# MAGIC It can then (conceptually) use the Databricks Jobs API to create or update this workflow.
# MAGIC
# MAGIC **Note:** Direct API calls to create/update jobs are placeholders in this version.
# MAGIC The primary goal here is to generate the correct Workflow JSON payload.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "PipelineID from config.Pipelines to generate workflow for")
dbutils.widgets.text("config_catalog", "main", "Unity Catalog for configuration tables")
dbutils.widgets.text("config_schema", "config", "Schema for configuration tables")
dbutils.widgets.text("bronze_notebook_path", "../etl_stages/bronze_ingestion_generic.py", "Path to Generic Bronze Ingestion Notebook")
dbutils.widgets.text("silver_notebook_path", "../etl_stages/silver_transformation_generic.py", "Path to Generic Silver Transformation Notebook")
dbutils.widgets.text("gold_notebook_path", "../etl_stages/gold_processing_shell.py", "Path to Gold Processing Shell Notebook")
dbutils.widgets.text("output_notebook_path", "../etl_stages/generic_data_output.py", "Path to Generic Data Output Notebook")

# For API interaction (placeholders)
dbutils.widgets.text("databricks_workspace_url", "https://<your-workspace-url>", "Databricks Workspace URL (e.g., adb-xxxx.azuredatabricks.net)")
dbutils.widgets.text("api_token_secret_scope", "databricks_secrets", "Secret scope for Databricks API token")
dbutils.widgets.text("api_token_secret_key", "api_token", "Secret key for Databricks API token")


# Retrieve widget values
pipeline_id = dbutils.widgets.get("pipeline_id")
config_catalog = dbutils.widgets.get("config_catalog")
config_schema = dbutils.widgets.get("config_schema")

bronze_notebook_path = dbutils.widgets.get("bronze_notebook_path")
silver_notebook_path = dbutils.widgets.get("silver_notebook_path")
gold_notebook_path = dbutils.widgets.get("gold_notebook_path")
output_notebook_path = dbutils.widgets.get("output_notebook_path")

databricks_workspace_url = dbutils.widgets.get("databricks_workspace_url")
api_token_secret_scope = dbutils.widgets.get("api_token_secret_scope")
api_token_secret_key = dbutils.widgets.get("api_token_secret_key")


# Construct full paths for config tables
CONFIG_PIPELINES_TABLE = f"{config_catalog}.{config_schema}.Pipelines"
CONFIG_DATASET_PIPELINE_MAPPING_TABLE = f"{config_catalog}.{config_schema}.DatasetPipelineMapping"
CONFIG_DATASETS_TABLE = f"{config_catalog}.{config_schema}.Datasets"

# Basic validation
if not pipeline_id:
    dbutils.notebook.exit("ERROR: pipeline_id widget must be provided.")

print(f"Generating workflow JSON for PipelineID: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Pipeline and Dataset Mapping Configuration

# COMMAND ----------

from pyspark.sql.functions import col, lit
import json

try:
    print(f"Reading Pipeline configuration for PipelineID: {pipeline_id}")
    pipeline_config_df = spark.read.table(CONFIG_PIPELINES_TABLE).where(col("PipelineID") == pipeline_id)
    pipeline_config = pipeline_config_df.first()

    if not pipeline_config:
        dbutils.notebook.exit(f"ERROR: No configuration found for PipelineID '{pipeline_id}' in {CONFIG_PIPELINES_TABLE}.")

    print(f"Pipeline configuration found: {pipeline_config.PipelineName}")

    print(f"Reading DatasetPipelineMapping for PipelineID: {pipeline_id}")
    dataset_mappings_df = spark.read.table(CONFIG_DATASET_PIPELINE_MAPPING_TABLE) \
        .where((col("PipelineID") == pipeline_id) & (col("IsEnabled") == True)) \
        .orderBy("ProcessingOrder") # Crucial for defining dependencies

    dataset_mappings = dataset_mappings_df.collect()
    if not dataset_mappings:
        dbutils.notebook.exit(f"ERROR: No enabled DatasetPipelineMapping found for PipelineID '{pipeline_id}'.")

    print(f"Found {len(dataset_mappings)} dataset mappings for this pipeline.")

    # Fetch details for all involved datasets
    dataset_ids_in_pipeline = [mapping.DatasetID for mapping in dataset_mappings]
    all_datasets_in_pipeline_df = spark.read.table(CONFIG_DATASETS_TABLE) \
        .where(col("DatasetID").isin(dataset_ids_in_pipeline) & (col("IsEnabled") == True))

    all_datasets_config = {row.DatasetID: row for row in all_datasets_in_pipeline_df.collect()}

    if len(all_datasets_config) != len(list(set(dataset_ids_in_pipeline))): # Compare with unique dataset_ids
        print("WARNING: Some datasets mapped to the pipeline are not found or not enabled in the Datasets table, or there are duplicate dataset IDs in the mapping.")
        # This could be an error condition depending on requirements.

except Exception as e:
    print(f"ERROR: Failed to read configuration: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construct Workflow JSON Payload
# MAGIC
# MAGIC This will generate the JSON structure for a Databricks Workflow.
# MAGIC Assumes a linear progression (Bronze -> Silver -> Gold -> Output) for each dataset,
# MAGIC with dependencies managed by ProcessingOrder for inter-dataset tasks if RunParallel is False.

# COMMAND ----------

workflow_json = {
    "name": pipeline_config.PipelineName,
    "email_notifications": {}, # Placeholder, can be filled from Notifications config later
    "timeout_seconds": pipeline_config.Parameters.get("timeout_seconds", 86400) if pipeline_config.Parameters and pipeline_config.Parameters.strip() else 86400, # Default to 24 hours
    "max_concurrent_runs": pipeline_config.Parameters.get("max_concurrent_runs", 1) if pipeline_config.Parameters and pipeline_config.Parameters.strip() else 1,
    "tasks": []
}

# Attempt to parse pipeline_config.Parameters as JSON if it's a string
pipeline_params_dict = {}
if pipeline_config.Parameters and pipeline_config.Parameters.strip():
    try:
        pipeline_params_dict = json.loads(pipeline_config.Parameters)
    except json.JSONDecodeError:
        print(f"WARNING: Could not parse pipeline-level Parameters JSON for PipelineID '{pipeline_id}'. Using defaults for timeout/concurrency/timezone if applicable.")
        pipeline_params_dict = {} # Ensure it's a dict

workflow_json["timeout_seconds"] = pipeline_params_dict.get("timeout_seconds", 86400)
workflow_json["max_concurrent_runs"] = pipeline_params_dict.get("max_concurrent_runs", 1)


if pipeline_config.ScheduleCronExpression:
    workflow_json["schedule"] = {
        "quartz_cron_expression": pipeline_config.ScheduleCronExpression,
        "timezone_id": pipeline_params_dict.get("timezone_id", "UTC")
    }

# Common parameters for all notebook tasks (can be overridden)
default_notebook_params = {
    "config_catalog": config_catalog,
    "config_schema": config_schema,
    # Bronze/Silver/Gold catalogs/schemas are taken from widgets for the generic notebooks
}

task_keys_by_dataset_id_and_layer = {} # {(dataset_id, layer): task_key}
last_task_key_at_processing_order = {} # {processing_order_value: task_key} (if not RunParallel)

run_parallel = pipeline_config.RunParallel

for mapping_idx, mapping in enumerate(dataset_mappings):
    dataset_id = mapping.DatasetID
    dataset_config = all_datasets_config.get(dataset_id)

    if not dataset_config:
        print(f"WARNING: Dataset config not found for DatasetID '{dataset_id}' in pipeline '{pipeline_id}'. Skipping this dataset.")
        continue

    dataset_name_sanitized = dataset_config.DatasetName.replace(" ", "_").replace("-", "_")
    processing_order = mapping.ProcessingOrder
    current_last_task_for_ds = None # Tracks the last task created for *this* dataset (Bronze, Silver, or Gold)

    # Bronze Task
    if dataset_config.BronzeLayerTargetTableName:
        bronze_task_key = f"{dataset_name_sanitized}_Bronze"
        bronze_params = default_notebook_params.copy()
        bronze_params.update({
            "dataset_id": dataset_id,
            "bronze_catalog": dbutils.widgets.get("bronze_catalog"), # from input widgets to this generator notebook
            "bronze_schema": dbutils.widgets.get("bronze_schema")   # from input widgets
        })
        if mapping.BronzeParameters and mapping.BronzeParameters.strip():
            try: bronze_params.update(json.loads(mapping.BronzeParameters))
            except json.JSONDecodeError: print(f"WARN: Invalid JSON in BronzeParameters for mapping {mapping.DatasetPipelineMappingID}")

        bronze_task = {
            "task_key": bronze_task_key,
            "notebook_task": {
                "notebook_path": bronze_notebook_path,
                "base_parameters": bronze_params
            },
            "depends_on": []
        }
        if not run_parallel and processing_order > 0 and (processing_order -1) in last_task_key_at_processing_order:
            bronze_task["depends_on"].append({"task_key": last_task_key_at_processing_order[processing_order-1]})

        workflow_json["tasks"].append(bronze_task)
        task_keys_by_dataset_id_and_layer[(dataset_id, "Bronze")] = bronze_task_key
        current_last_task_for_ds = bronze_task_key

    # Silver Task
    if dataset_config.SilverLayerTargetTableName:
        silver_task_key = f"{dataset_name_sanitized}_Silver"
        silver_params = default_notebook_params.copy()
        silver_params.update({
            "dataset_id": dataset_id,
            "bronze_catalog": dbutils.widgets.get("bronze_catalog"),
            "bronze_schema": dbutils.widgets.get("bronze_schema"),
            "silver_catalog": dbutils.widgets.get("silver_catalog"),
            "silver_schema": dbutils.widgets.get("silver_schema")
        })
        if mapping.SilverParameters and mapping.SilverParameters.strip():
            try: silver_params.update(json.loads(mapping.SilverParameters))
            except json.JSONDecodeError: print(f"WARN: Invalid JSON in SilverParameters for mapping {mapping.DatasetPipelineMappingID}")

        silver_task = {
            "task_key": silver_task_key,
            "notebook_task": {
                "notebook_path": silver_notebook_path,
                "base_parameters": silver_params
            },
            "depends_on": []
        }
        # Silver depends on its own Bronze if Bronze task exists for this dataset
        if task_keys_by_dataset_id_and_layer.get((dataset_id, "Bronze")):
            silver_task["depends_on"].append({"task_key": task_keys_by_dataset_id_and_layer[(dataset_id, "Bronze")]})
        # If no Bronze task for this dataset, and sequential mode, it depends on the previous dataset's last stage
        elif not run_parallel and processing_order > 0 and (processing_order - 1) in last_task_key_at_processing_order:
             silver_task["depends_on"].append({"task_key": last_task_key_at_processing_order[processing_order-1]})


        workflow_json["tasks"].append(silver_task)
        task_keys_by_dataset_id_and_layer[(dataset_id, "Silver")] = silver_task_key
        current_last_task_for_ds = silver_task_key

    # Gold Task
    if dataset_config.GoldLayerTargetTableName:
        gold_task_key = f"{dataset_name_sanitized}_Gold"
        gold_params = default_notebook_params.copy()
        gold_params.update({
            "dataset_id": dataset_id, # dataset_id here is for the *target* gold dataset config
            "silver_catalog": dbutils.widgets.get("silver_catalog"),
            "silver_schema": dbutils.widgets.get("silver_schema"),
            "gold_catalog": dbutils.widgets.get("gold_catalog"),
            "gold_schema": dbutils.widgets.get("gold_schema")
        })
        if mapping.GoldParameters and mapping.GoldParameters.strip():
            try: gold_params.update(json.loads(mapping.GoldParameters))
            except json.JSONDecodeError: print(f"WARN: Invalid JSON in GoldParameters for mapping {mapping.DatasetPipelineMappingID}")

        gold_task = {
            "task_key": gold_task_key,
            "notebook_task": {
                "notebook_path": gold_notebook_path,
                "base_parameters": gold_params
            },
            "depends_on": []
        }
        # Gold depends on its own Silver if Silver task exists for this dataset
        if task_keys_by_dataset_id_and_layer.get((dataset_id, "Silver")):
            gold_task["depends_on"].append({"task_key": task_keys_by_dataset_id_and_layer[(dataset_id, "Silver")]})
        # If no Silver task for this dataset (e.g. Gold derived directly from Bronze, or other Silvers)
        # and sequential mode, it depends on the previous dataset's last stage (if Bronze didn't exist for this DS either)
        # This part of dependency can get complex if Gold depends on multiple Silvers.
        # Current logic: depends on its own Silver, or if no own Silver, then own Bronze, else prev dataset.
        elif task_keys_by_dataset_id_and_layer.get((dataset_id, "Bronze")): # Depends on its own Bronze if no own Silver
             gold_task["depends_on"].append({"task_key": task_keys_by_dataset_id_and_layer[(dataset_id, "Bronze")]})
        elif not run_parallel and processing_order > 0 and (processing_order-1) in last_task_key_at_processing_order:
             gold_task["depends_on"].append({"task_key": last_task_key_at_processing_order[processing_order-1]})

        workflow_json["tasks"].append(gold_task)
        task_keys_by_dataset_id_and_layer[(dataset_id, "Gold")] = gold_task_key
        current_last_task_for_ds = gold_task_key

    # Update last task for this processing order slot (if sequential)
    if not run_parallel and current_last_task_for_ds: # Ensure a task was actually created for this dataset
        last_task_key_at_processing_order[processing_order] = current_last_task_for_ds


# Pretty print the JSON
workflow_json_str = json.dumps(workflow_json, indent=4)
print("\n--- Generated Workflow JSON ---")
print(workflow_json_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Databricks API Interaction (Placeholder)
# MAGIC
# MAGIC Functions to create or update the job in Databricks.

# COMMAND ----------

import requests

def get_databricks_api_token():
    try:
        token = dbutils.secrets.get(scope=api_token_secret_scope, key=api_token_secret_key)
        return token
    except Exception as e:
        # Check if it's a context-related error for local execution vs. Databricks
        if "dbutils is not defined" in str(e) or "Secret scope" in str(e) or "Secret not found" in str(e): # More specific checks
            print(f"INFO: dbutils.secrets.get not available or secret not found (Scope: '{api_token_secret_scope}', Key: '{api_token_secret_key}'). This is expected if running outside Databricks or if secrets are not configured for this run.")
        else:
            print(f"ERROR: Failed to retrieve API token. Error: {e}")
        return None


def find_existing_job_id(job_name, headers, workspace_url_internal):
    api_url = f"https://{workspace_url_internal}/api/2.1/jobs/list?name={job_name}" # Filter by name
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        if response.status_code == 200:
            jobs = response.json().get("jobs", [])
            if jobs: # If list is not empty, means a job with that name was found
                return jobs[0]["job_id"] # Return the first one if multiple (name is not unique constraint)
        else:
            print(f"API call to list jobs failed: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e_req:
        print(f"Request exception while listing jobs: {e_req}")
    return None

def create_or_update_workflow(workflow_payload_json_str):
    token = get_databricks_api_token()
    if not token:
        print("Skipping API interaction due to missing token.")
        return

    # Normalize workspace URL (remove https:// and trailing /)
    workspace_url_internal = databricks_workspace_url.lower().replace('https://','').rstrip('/')

    if "<your-workspace-url>" in workspace_url_internal or not workspace_url_internal:
        print("Placeholder or empty workspace URL detected. Skipping API interaction.")
        print("Please configure 'databricks_workspace_url' widget with your actual workspace URL.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    workflow_payload = json.loads(workflow_payload_json_str)
    job_name = workflow_payload["name"]

    existing_job_id = find_existing_job_id(job_name, headers, workspace_url_internal)

    if existing_job_id:
        print(f"Found existing job '{job_name}' with ID: {existing_job_id}. Attempting to update (reset).")
        api_url = f"https://{workspace_url_internal}/api/2.1/jobs/reset"
        reset_payload = {"job_id": existing_job_id, "new_settings": workflow_payload}
        try:
            response = requests.post(api_url, headers=headers, data=json.dumps(reset_payload), timeout=15)
            if response.status_code == 200:
                print(f"Successfully updated (reset) job '{job_name}' (ID: {existing_job_id}).")
            else:
                print(f"Failed to update job '{job_name}'. Status: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e_req:
            print(f"Request exception while updating job: {e_req}")
    else:
        print(f"No existing job found with name '{job_name}'. Attempting to create new job.")
        api_url = f"https://{workspace_url_internal}/api/2.1/jobs/create"
        try:
            response = requests.post(api_url, headers=headers, data=workflow_payload_json_str, timeout=15)
            if response.status_code == 200:
                job_id = response.json().get("job_id")
                print(f"Successfully created job '{job_name}' with ID: {job_id}.")
            else:
                print(f"Failed to create job '{job_name}'. Status: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e_req:
            print(f"Request exception while creating job: {e_req}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execute Generation and API Call (Conceptual)

# COMMAND ----------

# To run:
# 1. Fill in the 'pipeline_id' widget.
# 2. Ensure Databricks secret scope and key for API token are set up if you want to try API calls.
# 3. Ensure workspace URL is correct.

# create_or_update_workflow(workflow_json_str)
# The above line is commented out to prevent accidental API calls during automated runs.
# To actually create/update the workflow, uncomment it and ensure your API token and workspace URL are correctly configured.

print("\nWorkflow JSON generation complete. API call is commented out by default.")
dbutils.notebook.exit(f"Workflow JSON generated for PipelineID {pipeline_id}. See printed JSON. API interaction is conceptual in this version.")
