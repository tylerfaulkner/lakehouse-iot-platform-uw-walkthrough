# Databricks notebook source
# MAGIC %md
# MAGIC # Install the demo resources
# MAGIC
# MAGIC **Run this notebook once, before anything else.** It sets up everything the walkthrough needs in *your* workspace:
# MAGIC
# MAGIC | Resource | Name / location |
# MAGIC |---|---|
# MAGIC | Schema + volume with the raw turbine data | `<catalog>.<schema>.turbine_raw_landing` (see [config]($./config)) |
# MAGIC | Placeholder ML model (alias `@prod`) | `<catalog>.<schema>.dbdemos_turbine_maintenance` |
# MAGIC | `parts` and `maintenance_queue` tables | `<catalog>.<schema>` |
# MAGIC | Spark Declarative Pipeline (SQL version), run once | `dbdemos_sdp_iot_<catalog>_<schema>` |
# MAGIC | 2 AI/BI dashboards | your home folder, `dbdemos_iot_dashboards/` |
# MAGIC | Orchestration job (created, not run) | `dbdemos_iot_turbine_workflow_<catalog>_<schema>` |
# MAGIC | Databricks App (optional) | `wind-turbine-maintenance` |
# MAGIC
# MAGIC It also updates the links in the demo notebooks so they open *your* pipeline, dashboards, job and app.
# MAGIC
# MAGIC ### How to run
# MAGIC 1. Attach this notebook to **Serverless** compute (on Databricks Free Edition, that's the only option).
# MAGIC 2. Click **Run all**. The whole install takes about 15–30 minutes (most of it is the first pipeline run).
# MAGIC
# MAGIC Re-running is safe: existing resources are updated in place. Set `reset_all_data` to `true` to wipe the schema and start from scratch.
# MAGIC
# MAGIC *The default catalog is `workspace`, which exists in every Free Edition workspace. To use another catalog/schema, edit [config]($./config) first and re-run this notebook.*

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.dropdown("deploy_app", "true", ["true", "false"], "Deploy the Databricks App")

# COMMAND ----------

# MAGIC %pip install --quiet -U databricks-sdk mlflow==3.1.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Catalog, schema, volume, raw data and placeholder model

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

import base64, re, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, ObjectType

w = WorkspaceClient()
api = w.api_client
me = w.current_user.me().user_name
print(f"Demo folder: {repo_root}")
print(f"Installing into: {catalog}.{db}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS `{catalog}`.`{db}`.parts AS
              SELECT * FROM read_files('{volume_folder}/parts', format => 'json')""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Point the hardcoded paths at this install
# MAGIC
# MAGIC Some files can't read the config notebook (SDP pipeline sources, exploration notebooks, the app), so they contain the catalog/schema as plain text.
# MAGIC If you changed `config`, this rewrites them to match. With the default config, nothing changes.

# COMMAND ----------

TEXT_EXTENSIONS = (".py", ".sql", ".yaml", ".yml", ".txt", ".md")

def repo_objects():
  for o in w.workspace.list(repo_root, recursive=True):
    if o.object_type == ObjectType.NOTEBOOK or (o.object_type == ObjectType.FILE and o.path.endswith(TEXT_EXTENSIONS)):
      yield o

def read_object(o):
  fmt = ExportFormat.SOURCE if o.object_type == ObjectType.NOTEBOOK else ExportFormat.AUTO
  return base64.b64decode(w.workspace.export(o.path, format=fmt).content).decode("utf-8")

def write_object(o, content):
  b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
  if o.object_type == ObjectType.NOTEBOOK:
    w.workspace.import_(o.path, content=b64, format=ImportFormat.SOURCE, language=o.language, overwrite=True)
  else:
    w.workspace.import_(o.path, content=b64, format=ImportFormat.AUTO, overwrite=True)

def rewrite_repo(transform):
  changed = []
  for o in repo_objects():
    content = read_object(o)
    new_content = transform(content)
    if new_content != content:
      write_object(o, new_content)
      changed.append(o.path.replace(repo_root, ""))
  return changed

# The SQL bronze file is the reference: whatever catalog/schema it currently points to is what's written everywhere
bronze = read_object(next(o for o in repo_objects() if o.path.endswith("01.1-SDP-SQL/transformations/01-bronze.sql")))
old_catalog, old_schema = re.search(r"/Volumes/([A-Za-z0-9_\-]+)/([A-Za-z0-9_\-]+)/", bronze).groups()

if (old_catalog, old_schema) == (catalog, db):
  print(f"Hardcoded paths already point to {catalog}.{db}, nothing to change.")
else:
  print(f"Re-pointing hardcoded paths from {old_catalog}.{old_schema} to {catalog}.{db}...")
  dotted = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(old_catalog)}\.{re.escape(old_schema)}(?![A-Za-z0-9_])")
  def align_paths(text):
    text = text.replace(f"/Volumes/{old_catalog}/{old_schema}/", f"/Volumes/{catalog}/{db}/")
    text = dotted.sub(f"{catalog}.{db}", text)
    text = re.sub(rf'(CATALOG\s*=\s*"){re.escape(old_catalog)}(")', rf"\g<1>{catalog}\g<2>", text)
    text = re.sub(r'(name: "IOT_CATALOG"\s*\n\s*value: ")[^"]*(")', rf"\g<1>{catalog}\g<2>", text)
    text = re.sub(r'(name: "IOT_SCHEMA"\s*\n\s*value: ")[^"]*(")', rf"\g<1>{db}\g<2>", text)
    return text
  for f in rewrite_repo(align_paths):
    print(f"  updated {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Spark Declarative Pipeline
# MAGIC
# MAGIC Creates the SQL pipeline from `01-Data-ingestion/01.1-SDP-SQL` and runs it once to build the bronze/silver/gold tables. This is the longest step.

# COMMAND ----------

sdp_root = f"{repo_root}/01-Data-ingestion/01.1-SDP-SQL"
pipeline_name = f"dbdemos_sdp_iot_{catalog}_{db}"
pipeline_spec = {
  "name": pipeline_name,
  "catalog": catalog,
  "schema": db,
  "serverless": True,
  "continuous": False,
  "development": True,
  "channel": "CURRENT",
  "root_path": sdp_root,
  "libraries": [{"glob": {"include": f"{sdp_root}/transformations/**"}}],
  "environment": {"dependencies": ["mlflow==3.1.0"]},
}

existing = [p for p in api.do("GET", "/api/2.0/pipelines", query={"max_results": 100}).get("statuses", []) if p["name"] == pipeline_name]
if existing:
  pipeline_id = existing[0]["pipeline_id"]
  api.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={**pipeline_spec, "id": pipeline_id})
  print(f"Updated pipeline {pipeline_name} ({pipeline_id})")
else:
  pipeline_id = api.do("POST", "/api/2.0/pipelines", body=pipeline_spec)["pipeline_id"]
  print(f"Created pipeline {pipeline_name} ({pipeline_id})")

# COMMAND ----------

def run_pipeline(pipeline_id, timeout_min=45):
  try:
    update_id = api.do("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", body={"full_refresh": True})["update_id"]
  except Exception as e:
    if "active" not in str(e).lower():
      raise e
    #An update is already running (e.g. notebook re-run): wait for it instead
    update_id = api.do("GET", f"/api/2.0/pipelines/{pipeline_id}")["latest_updates"][0]["update_id"]
  print(f"Pipeline update {update_id} started, follow it here: #joblist/pipelines/{pipeline_id}")
  for i in range(timeout_min * 6):
    state = api.do("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")["update"]["state"]
    if state == "COMPLETED":
      print("Pipeline run completed.")
      return
    if state in ("FAILED", "CANCELED"):
      raise Exception(f"Pipeline update {state}. Open the pipeline to see the error: #joblist/pipelines/{pipeline_id}")
    if i % 12 == 0:
      print(f"  {state}...")
    time.sleep(10)
  raise Exception(f"Timeout waiting for the pipeline, check its status: #joblist/pipelines/{pipeline_id}")

run_pipeline(pipeline_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Maintenance queue table (used by the Databricks App)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{db}`.maintenance_queue AS
SELECT turbine_id,
       location,
       prediction                                          AS predicted_failure_type,
       CAST(abs(hash(turbine_id)) % 100 / 100.0 AS DOUBLE) AS severity_score,
       'Pending'                                           AS status,
       current_timestamp()                                 AS flagged_at,
       CAST(NULL AS STRING)                                AS technician_notes,
       current_timestamp()                                 AS last_updated
  FROM `{catalog}`.`{db}`.turbine_current_status
 WHERE prediction != 'ok'""")
display(spark.table(f"`{catalog}`.`{db}`.maintenance_queue").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5/ AI/BI dashboards

# COMMAND ----------

warehouse_id = get_shared_warehouse().id
dashboard_folder = f"/Users/{me}/dbdemos_iot_dashboards"
w.workspace.mkdirs(dashboard_folder)

def list_dashboards():
  page_token = None
  while True:
    r = api.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 100, **({"page_token": page_token} if page_token else {})})
    yield from r.get("dashboards", [])
    page_token = r.get("next_page_token")
    if not page_token:
      return

def install_dashboard(template, display_name):
  with open(f"/Workspace{repo_root}/_resources/dashboards/{template}.dashboard.json") as f:
    serialized = f.read().replace("`main`.`dbdemos_iot_platform`", f"`{catalog}`.`{db}`")
  in_folder = lambda d: re.sub(r"^/Workspace", "", d.get("parent_path") or d.get("path") or "").startswith(dashboard_folder)
  existing = [d for d in list_dashboards() if d.get("display_name") == display_name and in_folder(d)]
  body = {"display_name": display_name, "serialized_dashboard": serialized, "warehouse_id": warehouse_id}
  if existing:
    dashboard_id = existing[0]["dashboard_id"]
    api.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body=body)
  else:
    dashboard_id = api.do("POST", "/api/2.0/lakeview/dashboards", body={**body, "parent_path": dashboard_folder})["dashboard_id"]
  api.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", body={"warehouse_id": warehouse_id, "embed_credentials": True})
  print(f"Dashboard '{display_name}' ready: /sql/dashboardsv3/{dashboard_id}")
  return dashboard_id

dashboard_ids = {
  "turbine-analysis":   install_dashboard("turbine-analysis",   "[dbdemos] IOT - Turbine analysis"),
  "turbine-predictive": install_dashboard("turbine-predictive", "[dbdemos] IOT - Wind Turbine predictive maintenance"),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6/ Orchestration job
# MAGIC
# MAGIC Created for the [workflow orchestration notebook]($./06-Workflow-orchestration/06-Workflow-orchestration-iot-turbine), but not run. Start it from the Jobs UI if you want to see it in action.

# COMMAND ----------

job_name = f"dbdemos_iot_turbine_workflow_{catalog}_{db}"
job_settings = {
  "name": job_name,
  "max_concurrent_runs": 1,
  "tasks": [
    {"task_key": "init_data",
     "notebook_task": {"notebook_path": f"{repo_root}/_resources/01-load-data", "source": "WORKSPACE"}},
    {"task_key": "start_sdp_pipeline", "depends_on": [{"task_key": "init_data"}],
     "pipeline_task": {"pipeline_id": pipeline_id, "full_refresh": False}},
    {"task_key": "create_feature_and_train_model", "depends_on": [{"task_key": "start_sdp_pipeline"}],
     "notebook_task": {"notebook_path": f"{repo_root}/04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance", "source": "WORKSPACE"}},
  ],
}
existing = [j for j in api.do("GET", "/api/2.1/jobs/list", query={"name": job_name}).get("jobs", [])]
if existing:
  job_id = existing[0]["job_id"]
  api.do("POST", "/api/2.1/jobs/reset", body={"job_id": job_id, "new_settings": job_settings})
  print(f"Updated job {job_name} ({job_id})")
else:
  job_id = api.do("POST", "/api/2.1/jobs/create", body=job_settings)["job_id"]
  print(f"Created job {job_name} ({job_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7/ Databricks App (optional)
# MAGIC
# MAGIC Deploys `07-Databricks-apps/wind-turbine-maintenance` and gives the app's service principal access to the `maintenance_queue` table.
# MAGIC Apps keep running compute while they're up: stop the app from **Compute → Apps** when you're done.

# COMMAND ----------

app_name = "wind-turbine-maintenance"
app_url = None

def wait_for(get_state, ok_states, fail_states, what, timeout_min=20):
  for i in range(timeout_min * 6):
    state = get_state()
    if state in ok_states:
      return state
    if state in fail_states:
      raise Exception(f"{what} ended in state {state}")
    if i % 6 == 0:
      print(f"  {what}: {state}...")
    time.sleep(10)
  raise Exception(f"Timeout waiting for {what}")

if dbutils.widgets.get("deploy_app") == "true":
  try:
    try:
      app = api.do("GET", f"/api/2.0/apps/{app_name}")
    except Exception as e:
      if "does not exist" not in str(e).lower() and "not found" not in str(e).lower():
        raise e
      print(f"Creating app {app_name}...")
      app = api.do("POST", "/api/2.0/apps", body={"name": app_name, "description": "Wind turbine maintenance queue (IoT demo)"})

    if app.get("compute_status", {}).get("state") in ("STOPPED", "STOPPING"):
      api.do("POST", f"/api/2.0/apps/{app_name}/start")
    wait_for(lambda: api.do("GET", f"/api/2.0/apps/{app_name}").get("compute_status", {}).get("state"),
             ["ACTIVE"], ["ERROR"], "app compute")

    app = api.do("GET", f"/api/2.0/apps/{app_name}")
    sp = app["service_principal_client_id"]
    for grant in [f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp}`",
                  f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{db}` TO `{sp}`",
                  f"GRANT SELECT, MODIFY ON TABLE `{catalog}`.`{db}`.maintenance_queue TO `{sp}`"]:
      spark.sql(grant)

    deployment = api.do("POST", f"/api/2.0/apps/{app_name}/deployments",
                        body={"source_code_path": f"/Workspace{repo_root}/07-Databricks-apps/wind-turbine-maintenance", "mode": "SNAPSHOT"})
    wait_for(lambda: api.do("GET", f"/api/2.0/apps/{app_name}/deployments/{deployment['deployment_id']}")["status"]["state"],
             ["SUCCEEDED"], ["FAILED", "CANCELLED"], "app deployment")
    app_url = api.do("GET", f"/api/2.0/apps/{app_name}")["url"]
    print(f"App deployed: {app_url}")
  except Exception as e:
    print(f"WARN: couldn't deploy the app (the rest of the demo is installed). You can deploy it manually from Compute > Apps. Error: {e}")
else:
  print("Skipping app deployment (deploy_app=false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8/ Update the links in the demo notebooks

# COMMAND ----------

def set_link(text, attribute, value, href):
  return re.sub(rf'(<a\s+{attribute}="{re.escape(value)}"\s+href=")[^"]*(")', rf"\g<1>{href}\g<2>", text)

def update_links(text):
  if "dbdemos-" not in text:
    return text
  text = set_link(text, "dbdemos-pipeline-id", "sdp-sql", f"#joblist/pipelines/{pipeline_id}")
  for key, dashboard_id in dashboard_ids.items():
    text = set_link(text, "dbdemos-dashboard-id", key, f"/sql/dashboardsv3/{dashboard_id}")
  text = set_link(text, "dbdemos-workflow-id", "init-job", f"#job/{job_id}/tasks")
  if app_url:
    text = set_link(text, "dbdemos-app-id", app_name, app_url)
  return text

for f in rewrite_repo(update_links):
  print(f"  updated links in {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!

# COMMAND ----------

links = [
  ("Spark Declarative Pipeline", f"#joblist/pipelines/{pipeline_id}"),
  ("Dashboard: Turbine analysis", f"/sql/dashboardsv3/{dashboard_ids['turbine-analysis']}"),
  ("Dashboard: Predictive maintenance", f"/sql/dashboardsv3/{dashboard_ids['turbine-predictive']}"),
  ("Orchestration job", f"#job/{job_id}/tasks"),
] + ([("Databricks App", app_url)] if app_url else [])
displayHTML(f"""<h3>Demo installed in <code>{catalog}.{db}</code></h3><ul>""" +
            "".join(f'<li><a href="{href}" target="_blank">{label}</a></li>' for label, href in links) +
            """</ul><p>Next: open <b>00-IOT-wind-turbine-introduction-DI-platform</b> to start the walkthrough.</p>""")
