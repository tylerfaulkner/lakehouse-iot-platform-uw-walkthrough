# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC  
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1655539421928636&notebook=%2Fconfig&demo_name=lakehouse-iot-platform&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-iot-platform%2Fconfig&version=1">

# COMMAND ----------

#`workspace` is the default catalog of every Databricks Free Edition workspace.
#If you change catalog/schema here, re-run the 00-INSTALL-demo-resources notebook: it will re-point the hardcoded
#paths (SDP pipeline files, exploration notebooks, app) and re-create the pipeline, dashboards and job on the new location.

catalog = "workspace"
schema = dbName = db = "dbdemos_iot_platform"

secret_scope_name = "dbdemos"
secret_key_name = "ai_agent_sp_token"

MODEL_SERVING_ENDPOINT_NAME   = "dbdemos_iot_turbine_prediction_endpoint"
VECTOR_SEARCH_ENDPOINT_NAME   = "dbdemos_vs_endpoint"
FEATURE_SERVING_ENDPOINT_NAME = "dbdemos_iot_turbine_feature_endpoint"

volume_name = "turbine_raw_landing"
model_name = "dbdemos_turbine_maintenance"
agent_name = "dbdemos_agent_prescriptive_maintenance"