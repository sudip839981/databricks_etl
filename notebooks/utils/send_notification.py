# Databricks notebook source

# MAGIC %md
# MAGIC # Notification Utility (Simplified)
# MAGIC
# MAGIC This notebook sends notifications based on configurations in the `config.Notifications` table.
# MAGIC It's designed to be called by Databricks Workflows on events like success, failure, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "PipelineID that triggered this notification")
dbutils.widgets.text("event_type", "", "Event type (e.g., OnSuccess, OnFailure, OnStart, OnDQMWarning)")
dbutils.widgets.text("status_message", "Default status message", "Detailed status or error message")
dbutils.widgets.text("job_id", "0", "Databricks Job ID (from workflow)")
dbutils.widgets.text("run_id", "0", "Databricks Run ID (from workflow)")
dbutils.widgets.text("task_name", "N/A", "Optional: Specific task name that triggered the event")
dbutils.widgets.text("workspace_url", "N/A", "Databricks workspace URL (to build log links)")
dbutils.widgets.text("config_catalog", "main", "Unity Catalog for configuration tables")
dbutils.widgets.text("config_schema", "config", "Schema for configuration tables")

print("Simplified Notification Notebook Started")

# Retrieve widget values
pipeline_id_param = dbutils.widgets.get("pipeline_id")
event_type_param = dbutils.widgets.get("event_type")
status_message_param = dbutils.widgets.get("status_message")

print(f"PipelineID: {pipeline_id_param}")
print(f"EventType: {event_type_param}")
print(f"StatusMessage: {status_message_param}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Notification Configurations (Simplified)

# COMMAND ----------

# Placeholder for reading config
print("Placeholder for reading notification configurations.")
# Example:
# CONFIG_NOTIFICATIONS_TABLE = f"{dbutils.widgets.get('config_catalog')}.{dbutils.widgets.get('config_schema')}.Notifications"
# print(f"Would read from {CONFIG_NOTIFICATIONS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Process and Send Notifications (Simplified)

# COMMAND ----------

# Placeholder for sending logic
print("Placeholder for processing and sending notifications.")
# Example:
# print(f"Simulating sending notification for PipelineID {pipeline_id_param}, Event: {event_type_param}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Completion Summary (Simplified)

# COMMAND ----------

print("Simplified notification processing complete.")
dbutils.notebook.exit("Simplified notification processing finished.")
