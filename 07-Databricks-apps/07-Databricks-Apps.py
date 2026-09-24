# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Databricks Apps
# MAGIC
# MAGIC So far in this workshop, we've built pipelines, processed IoT data, and run predictive models — all inside Databricks. But how do you put that work in front of the people who actually need to act on it?
# MAGIC
# MAGIC **Databricks Apps** lets you build and host interactive web applications that live *directly inside your lakehouse* — no separate infrastructure, no copy-pasting data to another tool, no duct-taped integrations.
# MAGIC
# MAGIC Apps are built using python frameworks such as **Streamlit** and **Gradio** and run inside your Databricks workspace. They can query Delta tables, call ML models, and write data back — all with the same security and governance you already have.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 What Can You Build?
# MAGIC
# MAGIC Databricks Apps shine when you need to put live lakehouse data in front of a non-technical audience. Some real-world examples:
# MAGIC
# MAGIC - **Operational dashboards** — Let field teams monitor KPIs without needing Databricks access
# MAGIC - **Data quality tools** — Internal apps for data stewards to flag, review, and resolve issues
# MAGIC - **Model interfaces** — A front-end for an ML model where users can submit inputs and get predictions
# MAGIC - **Approval workflows** — Lightweight tools where a human reviews flagged records and takes action (sound familiar? 👀)
# MAGIC - **Self-serve reporting** — Let business users filter and export their own slices of data
# MAGIC - **Experiment tracking viewers** — Browse MLflow runs in a custom UI tailored to your team
# MAGIC
# MAGIC The key differentiator: your app is **colocated with your data**. No ETL to a separate DB, no stale exports, no extra auth layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌬️ Demo — Wind Turbine Maintenance Queue
# MAGIC
# MAGIC The app below is a live example of what we just built in this workshop.
# MAGIC
# MAGIC It represents what an **operations team** would use after our predictive maintenance pipeline runs — a queue of flagged turbines, severity scores, and a simple interface to update repair status and log technician notes. Everything you see is reading from and writing back to the Delta table we created earlier.
# MAGIC
# MAGIC 👉 **<a dbdemos-app-id="wind-turbine-maintenance" href="/compute/apps" target="_blank">Open the Turbine Maintenance Queue App</a>** *(the link is set by the `00-INSTALL-demo-resources` notebook; otherwise find the app under **Compute → Apps**)*
# MAGIC
# MAGIC > The first time you open the app, Databricks shows a **Permission Requested** screen asking whether the app may act on your behalf. Click **Authorize**. It only asks once. The first load can then take a minute or two while the app starts its serverless connection.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Syncing & Deploying with the Databricks CLI
# MAGIC
# MAGIC You don't have to manage your app code directly in the Databricks UI. The recommended workflow is to develop locally and deploy using the **Databricks CLI**.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC ```bash
# MAGIC pip install databricks-cli
# MAGIC databricks configure --token  # enter your workspace URL + personal access token
# MAGIC ```
# MAGIC
# MAGIC ### Project Structure
# MAGIC
# MAGIC A minimal Databricks App looks like this:
# MAGIC ```
# MAGIC my-app/
# MAGIC ├── app.py            # your Streamlit app
# MAGIC ├── app.yaml          # app config (entry command, env vars)
# MAGIC └── requirements.txt  # python dependencies
# MAGIC ```
# MAGIC
# MAGIC ### Sync Local Code to Databricks Workspace
# MAGIC ```bash
# MAGIC databricks sync --watch ./my-app /Workspace/Users/you@email.com/my-app
# MAGIC ```
# MAGIC
# MAGIC `--watch` keeps it running and pushes changes as you save — great for iterative development.
# MAGIC
# MAGIC ### Deploy the App
# MAGIC ```bash
# MAGIC databricks apps deploy my-app --source-code-path /Workspace/Users/you@email.com/my-app
# MAGIC ```
# MAGIC
# MAGIC ### Check Status
# MAGIC ```bash
# MAGIC databricks apps get my-app
# MAGIC ```
# MAGIC
# MAGIC Once deployed, your app will be available at a URL inside your workspace. You can find it under **Compute → Apps** in the Databricks UI.
# MAGIC
# MAGIC > 📖 Full docs: [Databricks Apps Documentation](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)