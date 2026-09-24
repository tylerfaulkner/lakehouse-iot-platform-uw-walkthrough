# Databricks notebook source
# MAGIC %md
# MAGIC # Walkthrough - Run an ML Experiment
# MAGIC
# MAGIC In this hands-on portion you will configure an ML experiment through code then use the Experminets UI to examine results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install the Feature Engineering Client and MLFlow

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sdk==0.59.0 mlflow==3.1.1 databricks-feature-engineering==0.12.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Enter Your Email and Customize Hyper Parameters for Search
# MAGIC
# MAGIC You're email will be needed to ensure that the experminet run lands in you user folder.
# MAGIC
# MAGIC Feel free to change the hyperparameters how you like.

# COMMAND ----------

# Your email is detected automatically (you can also type it in directly)
YOUR_EMAIL = spark.sql("SELECT current_user()").first()[0]

# Hyperparameter search bounds — tune these to explore!
PARAM_BOUNDS = {
    "n_estimators":  [50, 100, 150, 200],          # number of trees
    "max_depth":     [3, 5, 7],               # tree depth — higher = more complex
    "learning_rate": [0.01, 0.05, 0.1, 0.2], # step size — lower = slower but safer
    "subsample":     [0.6, 0.8, 1.0],         # fraction of data per tree
}

# How many random combos to try (more = better results, longer runtime)
N_SEARCH_ITER = 10

# COMMAND ----------

from datetime import datetime
import itertools, random
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from mlflow.tracking import MlflowClient


assert YOUR_EMAIL != "your.email@company.com", "❌ Don't forget to set YOUR_EMAIL above!"


# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# catalog and schema come from the config notebook at the root of the repo
xp_path     = f"/Workspace/Users/{YOUR_EMAIL}/iot_turbine_experiments"
xp_name      = f"turbine_hpsearch_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
full_xp_name = f"{xp_path}/{xp_name}"

print(f"📁 Experiment will be logged to: {full_xp_name}")

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare the Dataset for the Experiments
# MAGIC
# MAGIC This code should look familar as it is nearly the same code we walked through together earlier.

# COMMAND ----------

training_dataset = (
    fe.read_table(name=f'{catalog}.{schema}.turbine_hourly_features')
    .drop('turbine_id')
    .sample(0.1)
    .toPandas()
)

target_col    = "abnormal_sensor"
timestamp_col = "hourly_timestamp"

# Time feature engineering
training_dataset[timestamp_col] = pd.to_datetime(training_dataset[timestamp_col])
training_dataset = training_dataset.sort_values(timestamp_col)

training_dataset["ts_hour"]       = training_dataset[timestamp_col].dt.hour
training_dataset["ts_dayofweek"]  = training_dataset[timestamp_col].dt.dayofweek
training_dataset["ts_month"]      = training_dataset[timestamp_col].dt.month
training_dataset["ts_is_weekend"] = (training_dataset["ts_dayofweek"] >= 5).astype(int)
training_dataset["ts_quarter"]    = training_dataset[timestamp_col].dt.quarter
training_dataset = training_dataset.drop(columns=[timestamp_col])

X = training_dataset.drop(columns=[target_col])
y = training_dataset[target_col]

if y.dtype == object:
    le = LabelEncoder()
    y  = le.fit_transform(y)
    print(f"🏷️  Target classes: {list(le.classes_)}")

# Auto-detect column types
string_cols  = X.select_dtypes(include=["object", "category"]).columns.tolist()
numeric_cols = X.select_dtypes(exclude=["object", "category"]).columns.tolist()

print(f"🔤 String cols  : {string_cols}")
print(f"🔢 Numeric cols : {numeric_cols}")
print(f"📊 Dataset shape: {X.shape}")

# Temporal split
split_idx = int(len(X) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

print(f"🕐 Train rows: {len(X_train)} | Test rows: {len(X_test)}")


# COMMAND ----------

preprocessor = ColumnTransformer(
    transformers=[
        ("ordinal", OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1
        ), string_cols),
    ],
    remainder="passthrough"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the Experiments
# MAGIC
# MAGIC Again, this should look familar to our experiment code from the demo; however, we are now enumarating over the hyper parameters you specified earlier in the notebook.

# COMMAND ----------

# Build all combos then randomly sample N_SEARCH_ITER of them
all_combos = list(itertools.product(*PARAM_BOUNDS.values()))
param_keys  = list(PARAM_BOUNDS.keys())

random.seed(42)
sampled_combos = random.sample(all_combos, min(N_SEARCH_ITER, len(all_combos)))

print(f"🔍 Running {len(sampled_combos)} trials out of {len(all_combos)} possible combinations\n")

dbutils.fs.mkdirs(xp_path)

mlflow.set_experiment(full_xp_name)

for i, combo in enumerate(sampled_combos):
    params = dict(zip(param_keys, combo))

    with mlflow.start_run(run_name=f"trial_{i+1:02d}"):

        pipeline = Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("classifier",   GradientBoostingClassifier(
                **params,
                random_state=42
            ))
        ])

        pipeline.fit(X_train, y_train)

        y_pred  = pipeline.predict(X_test)
        y_proba = pipeline.predict_proba(X_test)

        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "f1_score": f1_score(y_test, y_pred, average="weighted"),
            "roc_auc":  roc_auc_score(y_test, y_proba, multi_class="ovr", average="weighted"),
        }

        # Log everything
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.set_tags({
            "student":        YOUR_EMAIL,
            "split_strategy": "temporal_80_20",
            "encoder":        "OrdinalEncoder",
            "model_type":     "GradientBoostingClassifier",
        })

        signature = infer_signature(X_train, y_pred)
        mlflow.sklearn.log_model(
            pipeline,
            artifact_path="model",
            signature=signature,
            input_example=X_train.head(5),
        )

        print(
            f"Trial {i+1:02d} | "
            f"n_est={params['n_estimators']:>3} | "
            f"depth={params['max_depth']} | "
            f"lr={params['learning_rate']} | "
            f"subsample={params['subsample']} || "
            f"AUC={metrics['roc_auc']:.4f} | "
            f"F1={metrics['f1_score']:.4f} | "
            f"Acc={metrics['accuracy']:.4f}"
        )

experiment_id = mlflow.last_active_run().info.experiment_id
print(f"🔑 Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract the Best Model using Code

# COMMAND ----------

runs_df = mlflow.search_runs(
    experiment_ids=[experiment_id],
    filter_string=f"tags.student = '{YOUR_EMAIL}'",
    order_by=["metrics.roc_auc DESC"]
)

display(
    runs_df[[
        "run_id",
        "params.n_estimators",
        "params.max_depth",
        "params.learning_rate",
        "params.subsample",
        "metrics.roc_auc",
        "metrics.f1_score",
        "metrics.accuracy",
    ]].rename(columns=lambda c: c.replace("params.", "").replace("metrics.", ""))
)

best_run = runs_df.iloc[0]
print(f"\n🏆 Best run  : {best_run['run_id']}")
print(f"   ROC AUC  : {best_run['metrics.roc_auc']:.4f}")
print(f"   F1 Score : {best_run['metrics.f1_score']:.4f}")
print(f"   Params   : n_estimators={best_run['params.n_estimators']} | "
      f"max_depth={best_run['params.max_depth']} | "
      f"lr={best_run['params.learning_rate']} | "
      f"subsample={best_run['params.subsample']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compare your runs in the Experiments section in Databricks 
# MAGIC
# MAGIC ![image_1773345564467.png](./image_1773345564467.png "image_1773345564467.png")
# MAGIC
# MAGIC ### a. Find Your Experiment (should be on top)
# MAGIC ![image_1773349714273.png](./image_1773349714273.png "image_1773349714273.png")
# MAGIC
# MAGIC ### b. Select all Trials + Compare
# MAGIC
# MAGIC ![image_1773349739940.png](./image_1773349739940.png "image_1773349739940.png")
# MAGIC
# MAGIC ### c. Explore the UI and see how you models compare to each other
# MAGIC
# MAGIC ![image_1773349801178.png](./image_1773349801178.png "image_1773349801178.png")