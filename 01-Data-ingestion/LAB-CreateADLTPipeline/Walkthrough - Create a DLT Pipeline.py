# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On - Create a DLT Pipeline
# MAGIC
# MAGIC Please note, we will be going through this together during the hands-on portion of the workshop. This notebook is purely to document the process for later reference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Go To Jobs & Pipelines Section of Databricks
# MAGIC  ![image_1773244447007.png](./image_1773244447007.png "image_1773244447007.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select Create then Select ETL Pipeline
# MAGIC ![image_1773245659429.png](./image_1773245659429.png "image_1773245659429.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Select 'Add Existing Assets'
# MAGIC
# MAGIC ![image_1773254010639.png](./image_1773254010639.png "image_1773254010639.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Select 01.1-SDP-SQL as the root folder
# MAGIC
# MAGIC ![image_1773254072753.png](./image_1773254072753.png "image_1773254072753.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Select the 'transformations' folder as a Source Code path
# MAGIC
# MAGIC ![image_1773254160231.png](./image_1773254160231.png "image_1773254160231.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. In Pipeline Settings Add mlfow as a requirement for UDF
# MAGIC
# MAGIC > **Note:** keep the pipeline's **Default schema** on `default` (or any schema other than `dbdemos_iot_platform`). The pipeline created by the `00-INSTALL-demo-resources` notebook already owns the tables in `dbdemos_iot_platform`, and two pipelines can't write the same tables. Your pipeline still reads the raw files from the demo volume.
# MAGIC
# MAGIC ![image_1773254380322.png](./image_1773254380322.png "image_1773254380322.png")
# MAGIC
# MAGIC ![image_1773253880624.png](./image_1773253880624.png "image_1773253880624.png")
# MAGIC
# MAGIC Copy and paste the following: `mlflow==3.1.0`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. In the top right select 'Dry Run' to see your pipeline gewt validated and a graph get generated
# MAGIC ![image_1773254533154.png](./image_1773254533154.png "image_1773254533154.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulaions! You have built your first ETL Pipeline
# MAGIC You should now see a graph like below after a few minutes in the Jobs & Pipelines view 
# MAGIC ![image_1773254879258.png](./image_1773254879258.png "image_1773254879258.png")