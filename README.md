# Databricks walkthrough: IoT wind turbine predictive maintenance

A hands-on introduction to the Databricks Data Intelligence Platform, based on the
[dbdemos](https://github.com/databricks-demos/dbdemos) `lakehouse-iot-platform` demo. You'll ingest
wind turbine sensor data with Spark Declarative Pipelines, govern it with Unity Catalog, analyze it with
AI/BI dashboards, train a predictive maintenance model, build AI agent tools, orchestrate it all with a
job, and serve it through a Databricks App.

## Setup (about 30 minutes, mostly waiting)

### 1. Create a free Databricks account
Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition). No credit card needed.

### 2. Clone this repo into your workspace
1. In your workspace, open **Workspace** in the left sidebar, then go to your home folder.
2. Click **Create → Git folder**.
3. Paste this repository's URL, leave the provider as **GitHub**, and click **Create Git folder**.

No GitHub account or token is needed to clone a public repo.

### 3. Install the demo resources
1. Open the **`00-INSTALL-demo-resources`** notebook at the root of the Git folder.
2. Attach it to **Serverless** compute and click **Run all**.

The installer creates everything in the `workspace` catalog, schema `dbdemos_iot_platform`:

- the volume with the raw turbine data, plus the `parts`, `maintenance_queue` and pipeline tables
- a placeholder ML model `dbdemos_turbine_maintenance@prod`, used by the pipeline
- the Spark Declarative Pipeline `dbdemos_sdp_iot_workspace_dbdemos_iot_platform`, run once
- two AI/BI dashboards, in your home folder under `dbdemos_iot_dashboards/`
- the orchestration job `dbdemos_iot_turbine_workflow_workspace_dbdemos_iot_platform`, created but not run
- the `wind-turbine-maintenance` Databricks App (set the `deploy_app` widget to `false` to skip it)

It also updates the links inside the notebooks so they open *your* pipeline, dashboards, job and app.
Git will then show those notebooks as modified. That's expected, and you don't need to commit them.

### Getting updates later
If the repo is updated after you installed, open the Git dialog (the branch name next to the folder
title) and click **Pull**. If Pull reports a conflict, the conflicting files are the installer's link
updates:

1. Click the **⋮** menu next to *N changed files*, choose **Discard all changes**, and confirm.
2. Click **Pull**.
3. Re-run `00-INSTALL-demo-resources` to restore your links and pick up any other updates.

### 4. Start the walkthrough
Open **`00-IOT-wind-turbine-introduction-DI-platform`** and follow along.

## Using a different catalog or schema
All notebooks read the catalog and schema from [`config`](config.py). To change them, edit `config`
and re-run `00-INSTALL-demo-resources`. It re-points the files that can't read `config` (the SDP pipeline
sources, the exploration notebooks and the app) and recreates the resources in the new location.

## Free Edition notes
- Everything runs on serverless compute. There's no cluster to create.
- Every notebook has been tested end to end on a fresh Free Edition workspace, including model serving
  (`04.2`), Lakebase and Vector Search (`05.1`).
- Some steps take a while the first time: the installer takes about 15–30 minutes, and `05.1` takes about
  25 minutes while it provisions the Lakebase database and the Vector Search endpoint and index.
- The labs' `TODO` cells are meant to fail until you fill them in.
- The first time you open the Databricks App, you'll see a **Permission Requested** screen. Click
  **Authorize**; it only asks once.
- The Databricks App keeps compute running while it's up. Stop it from **Compute → Apps** when you're done.

## Repo layout
| Folder | What's inside |
|---|---|
| `00-INSTALL-demo-resources` | One-time installer. Run this first. |
| `00-IOT-wind-turbine-introduction-DI-platform` | Start of the walkthrough |
| `01-Data-ingestion` | SDP pipelines (SQL and Python), plain Spark version, pipeline lab |
| `02-Data-governance` | Unity Catalog governance |
| `03-BI-data-warehousing` | Databricks SQL and dashboards, dashboard lab |
| `04-Data-Science-ML` | Model training and inference, MLflow experiment lab |
| `05-Generative-AI` | AI agent tools, agent tools lab |
| `06-Workflow-orchestration` | Jobs |
| `07-Databricks-apps` | Streamlit maintenance-queue app |
| `_resources` | Setup and data-generation notebooks, dashboard templates |

Demo content © Databricks, Inc., released under the [Databricks License](_resources/LICENSE.py).
